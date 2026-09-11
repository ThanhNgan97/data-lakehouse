# -*- coding: utf-8 -*-
"""Safe, non-executing MySQL dump parser for KPI imports.

Supported input (v1):
- CREATE DATABASE [IF NOT EXISTS]
- USE
- CREATE TABLE [IF NOT EXISTS]
- INSERT INTO ... [(columns...)] VALUES (...), (...)

All other statements are ignored and counted. Nothing in the dump is executed.
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

SUPPORTED_KPI_TABLES = ("don_vi", "muc_tieu_kpi", "ket_qua_danh_gia")

_IDENTIFIER = r"(?:`(?:``|[^`])+`|[A-Za-z0-9_$\u0080-\uffff]+)"
_QUALIFIED_IDENTIFIER = rf"{_IDENTIFIER}(?:\s*\.\s*{_IDENTIFIER})?"


def _unquote_identifier(value: str) -> str:
    value = value.strip()
    if "." in value:
        value = re.split(r"\s*\.\s*", value)[-1]
    if value.startswith("`") and value.endswith("`"):
        value = value[1:-1].replace("``", "`")
    return value


def _statement_type(statement: str) -> str:
    s = statement.lstrip()
    if not s:
        return "EMPTY"
    # MySQL versioned comments are metadata/session directives in dumps.
    if s.startswith("/*!"):
        return "MYSQL_DIRECTIVE"
    m = re.match(r"([A-Za-z]+)(?:\s+([A-Za-z]+))?", s)
    if not m:
        return "OTHER"
    first = m.group(1).upper()
    second = (m.group(2) or "").upper()
    if first in {"CREATE", "DROP", "ALTER", "LOCK", "UNLOCK", "SET", "FLUSH"} and second:
        return f"{first}_{second}"
    return first


def _strip_comments(sql: str) -> str:
    """Remove --, # and /* */ comments without touching quoted text."""
    out: List[str] = []
    i = 0
    n = len(sql)
    quote: Optional[str] = None
    while i < n:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ""

        if quote:
            out.append(ch)
            if ch == "\\" and quote in {"'", '"'} and i + 1 < n:
                out.append(sql[i + 1])
                i += 2
                continue
            if ch == quote:
                # doubled quote/backtick escape
                if i + 1 < n and sql[i + 1] == quote:
                    out.append(sql[i + 1])
                    i += 2
                    continue
                quote = None
            i += 1
            continue

        if ch in {"'", '"', "`"}:
            quote = ch
            out.append(ch)
            i += 1
            continue

        if ch == "/" and nxt == "*":
            end = sql.find("*/", i + 2)
            if end == -1:
                break
            out.append(" ")
            i = end + 2
            continue

        if ch == "#":
            end = sql.find("\n", i + 1)
            if end == -1:
                break
            out.append("\n")
            i = end + 1
            continue

        # MySQL -- comment requires whitespace/control after second dash.
        if ch == "-" and nxt == "-" and (i + 2 >= n or sql[i + 2].isspace()):
            end = sql.find("\n", i + 2)
            if end == -1:
                break
            out.append("\n")
            i = end + 1
            continue

        out.append(ch)
        i += 1

    return "".join(out)


def split_sql_statements(sql: str) -> List[str]:
    """Split on semicolons outside quoted strings/backticks."""
    statements: List[str] = []
    buf: List[str] = []
    quote: Optional[str] = None
    i = 0
    n = len(sql)
    while i < n:
        ch = sql[i]
        buf.append(ch)
        if quote:
            if ch == "\\" and quote in {"'", '"'} and i + 1 < n:
                buf.append(sql[i + 1])
                i += 2
                continue
            if ch == quote:
                if i + 1 < n and sql[i + 1] == quote:
                    buf.append(sql[i + 1])
                    i += 2
                    continue
                quote = None
            i += 1
            continue

        if ch in {"'", '"', "`"}:
            quote = ch
        elif ch == ";":
            stmt = "".join(buf[:-1]).strip()
            if stmt:
                statements.append(stmt)
            buf = []
        i += 1

    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)
    return statements


def _split_top_level(text: str, delimiter: str = ",") -> List[str]:
    parts: List[str] = []
    buf: List[str] = []
    quote: Optional[str] = None
    depth = 0
    i = 0
    while i < len(text):
        ch = text[i]
        if quote:
            buf.append(ch)
            if ch == "\\" and quote in {"'", '"'} and i + 1 < len(text):
                buf.append(text[i + 1])
                i += 2
                continue
            if ch == quote:
                if i + 1 < len(text) and text[i + 1] == quote:
                    buf.append(text[i + 1])
                    i += 2
                    continue
                quote = None
            i += 1
            continue

        if ch in {"'", '"', "`"}:
            quote = ch
            buf.append(ch)
        elif ch == "(":
            depth += 1
            buf.append(ch)
        elif ch == ")":
            depth = max(0, depth - 1)
            buf.append(ch)
        elif ch == delimiter and depth == 0:
            parts.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
        i += 1

    if buf or text.strip():
        parts.append("".join(buf).strip())
    return parts


def _extract_create_table_columns(statement: str) -> Optional[Tuple[str, List[str]]]:
    m = re.match(
        rf"\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?P<table>{_QUALIFIED_IDENTIFIER})\s*\(",
        statement,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not m:
        return None
    table = _unquote_identifier(m.group("table"))
    open_idx = m.end() - 1

    quote: Optional[str] = None
    depth = 0
    close_idx = None
    i = open_idx
    while i < len(statement):
        ch = statement[i]
        if quote:
            if ch == "\\" and quote in {"'", '"'}:
                i += 2
                continue
            if ch == quote:
                if i + 1 < len(statement) and statement[i + 1] == quote:
                    i += 2
                    continue
                quote = None
            i += 1
            continue
        if ch in {"'", '"', "`"}:
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                close_idx = i
                break
        i += 1
    if close_idx is None:
        raise ValueError(f"CREATE TABLE '{table}' không đóng dấu ngoặc.")

    body = statement[open_idx + 1 : close_idx]
    definitions = _split_top_level(body)
    columns: List[str] = []
    skip_prefixes = (
        "PRIMARY ", "UNIQUE ", "KEY ", "INDEX ", "CONSTRAINT ", "FOREIGN ",
        "CHECK ", "FULLTEXT ", "SPATIAL ",
    )
    for definition in definitions:
        d = definition.strip()
        upper = d.upper()
        if not d or upper.startswith(skip_prefixes):
            continue
        cm = re.match(rf"(?P<col>{_IDENTIFIER})\s+", d, flags=re.IGNORECASE | re.DOTALL)
        if cm:
            columns.append(_unquote_identifier(cm.group("col")))
    return table, columns


def _iter_value_tuples(values_text: str) -> Iterable[str]:
    """Yield only the leading VALUES tuple list; ignore trailing ON DUPLICATE clauses."""
    i = 0
    n = len(values_text)
    yielded = False
    while True:
        while i < n and values_text[i].isspace():
            i += 1
        if i >= n or values_text[i] != "(":
            if not yielded:
                raise ValueError("INSERT VALUES không bắt đầu bằng tuple '(...)'.")
            return

        start = i + 1
        depth = 1
        quote: Optional[str] = None
        i += 1
        while i < n:
            ch = values_text[i]
            if quote:
                if ch == "\\" and quote in {"'", '"'} and i + 1 < n:
                    i += 2
                    continue
                if ch == quote:
                    if i + 1 < n and values_text[i + 1] == quote:
                        i += 2
                        continue
                    quote = None
                i += 1
                continue
            if ch in {"'", '"'}:
                quote = ch
            elif ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    yield values_text[start:i]
                    yielded = True
                    i += 1
                    break
            i += 1
        else:
            raise ValueError("INSERT VALUES có dấu ngoặc chưa đóng.")

        while i < n and values_text[i].isspace():
            i += 1
        if i < n and values_text[i] == ",":
            j = i + 1
            while j < n and values_text[j].isspace():
                j += 1
            if j < n and values_text[j] == "(":
                i = j
                continue
        return


_MYSQL_ESCAPES = {
    "0": "\0", "b": "\b", "n": "\n", "r": "\r", "t": "\t",
    "Z": "\x1a", "\\": "\\", "'": "'", '"': '"',
}


def _decode_mysql_string(token: str) -> str:
    quote = token[0]
    inner = token[1:-1]
    inner = inner.replace(quote + quote, quote)
    out: List[str] = []
    i = 0
    while i < len(inner):
        ch = inner[i]
        if ch == "\\" and i + 1 < len(inner):
            nxt = inner[i + 1]
            out.append(_MYSQL_ESCAPES.get(nxt, nxt))
            i += 2
        else:
            out.append(ch)
            i += 1
    return "".join(out)


def _parse_literal(token: str):
    t = token.strip()
    if not t:
        return ""
    if t.upper() == "NULL":
        return None
    # charset introducer such as _utf8mb4'...'
    intro = re.match(r"^_[A-Za-z0-9]+\s*('.*'|\".*\")$", t, flags=re.DOTALL)
    if intro:
        t = intro.group(1)
    if len(t) >= 2 and t[0] in {"'", '"'} and t[-1] == t[0]:
        return _decode_mysql_string(t)
    # Preserve numeric/expression tokens as text; Silver owns normalization.
    return t


def _parse_insert(statement: str, known_columns: Dict[str, List[str]]) -> Optional[Tuple[str, List[dict]]]:
    m = re.match(
        rf"\s*INSERT\s+(?:IGNORE\s+)?INTO\s+(?P<table>{_QUALIFIED_IDENTIFIER})\s*",
        statement,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not m:
        return None
    table = _unquote_identifier(m.group("table"))
    pos = m.end()
    rest = statement[pos:].lstrip()
    columns: Optional[List[str]] = None

    if rest.startswith("("):
        quote: Optional[str] = None
        depth = 0
        close = None
        for idx, ch in enumerate(rest):
            if quote:
                if ch == quote:
                    quote = None
                continue
            if ch == "`":
                quote = "`"
            elif ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    close = idx
                    break
        if close is None:
            raise ValueError(f"INSERT INTO '{table}' có danh sách cột không hợp lệ.")
        columns = [_unquote_identifier(c) for c in _split_top_level(rest[1:close])]
        rest = rest[close + 1 :].lstrip()

    vm = re.match(r"VALUES\b", rest, flags=re.IGNORECASE)
    if not vm:
        # INSERT ... SELECT is deliberately unsupported in v1.
        return table, []
    values_text = rest[vm.end() :]

    if columns is None:
        columns = known_columns.get(table)
        if not columns:
            raise ValueError(
                f"INSERT INTO '{table}' không có danh sách cột và parser chưa đọc được CREATE TABLE tương ứng."
            )

    rows: List[dict] = []
    for tuple_text in _iter_value_tuples(values_text):
        values = [_parse_literal(v) for v in _split_top_level(tuple_text)]
        if len(values) != len(columns):
            raise ValueError(
                f"INSERT INTO '{table}' có {len(values)} giá trị nhưng {len(columns)} cột."
            )
        rows.append(dict(zip(columns, values)))
    return table, rows


def parse_mysql_dump_text(
    sql_text: str,
    *,
    required_tables: Sequence[str] = SUPPORTED_KPI_TABLES,
    allowed_tables: Sequence[str] = SUPPORTED_KPI_TABLES,
) -> dict:
    """Parse a MySQL dump as data only. This function never executes SQL."""
    if not isinstance(sql_text, str) or not sql_text.strip():
        raise ValueError("SQL dump rỗng.")

    cleaned = _strip_comments(sql_text.lstrip("\ufeff"))
    statements = split_sql_statements(cleaned)
    known_columns: Dict[str, List[str]] = {}
    rows_by_table: Dict[str, List[dict]] = {t: [] for t in allowed_tables}
    database: Optional[str] = None
    ignored: Dict[str, int] = {}
    warnings: List[str] = []
    allowed = set(allowed_tables)

    for statement in statements:
        s = statement.strip()
        if not s:
            continue

        dbm = re.match(
            rf"CREATE\s+DATABASE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?P<db>{_IDENTIFIER})",
            s,
            flags=re.IGNORECASE,
        )
        if dbm:
            database = _unquote_identifier(dbm.group("db"))
            continue

        usem = re.match(rf"USE\s+(?P<db>{_IDENTIFIER})\s*$", s, flags=re.IGNORECASE)
        if usem:
            database = _unquote_identifier(usem.group("db"))
            continue

        ct = _extract_create_table_columns(s)
        if ct:
            table, cols = ct
            known_columns[table] = cols
            if table not in allowed:
                warnings.append(f"Bỏ qua schema bảng ngoài phạm vi v1: {table}")
            continue

        ins = _parse_insert(s, known_columns)
        if ins:
            table, rows = ins
            if table in allowed:
                rows_by_table.setdefault(table, []).extend(rows)
            else:
                warnings.append(f"Bỏ qua dữ liệu bảng ngoài phạm vi v1: {table}")
            continue

        kind = _statement_type(s)
        ignored[kind] = ignored.get(kind, 0) + 1

    missing = [t for t in required_tables if not rows_by_table.get(t)]
    if missing:
        raise ValueError(
            "SQL dump không chứa dữ liệu của các bảng KPI bắt buộc: " + ", ".join(missing)
        )

    return {
        "database": database,
        "tables": {
            table: {
                "columns": known_columns.get(table, []),
                "rows": rows_by_table.get(table, []),
                "row_count": len(rows_by_table.get(table, [])),
            }
            for table in allowed_tables
        },
        "ignored_statement_types": dict(sorted(ignored.items())),
        "warnings": warnings,
        "executed_sql": False,
    }


def parse_mysql_dump_file(path: str | Path, *, max_bytes: int = 50 * 1024 * 1024) -> dict:
    p = Path(path)
    if p.suffix.lower() != ".sql":
        raise ValueError("Chỉ hỗ trợ file .sql trong SQL dump parser.")
    size = p.stat().st_size
    if size <= 0:
        raise ValueError("SQL dump rỗng.")
    if size > max_bytes:
        raise ValueError(f"SQL dump vượt giới hạn {max_bytes // (1024 * 1024)} MB.")
    raw = p.read_bytes()
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("SQL dump phải dùng UTF-8/UTF-8 BOM trong phiên bản đầu tiên.") from exc
    return parse_mysql_dump_text(text)


def main() -> None:
    parser = argparse.ArgumentParser(description="Safe MySQL dump parser (no SQL execution)")
    parser.add_argument("path", help="Đường dẫn file .sql")
    parser.add_argument("--max-mb", type=int, default=50)
    args = parser.parse_args()
    result = parse_mysql_dump_file(args.path, max_bytes=args.max_mb * 1024 * 1024)
    summary = {
        "database": result["database"],
        "row_counts": {k: v["row_count"] for k, v in result["tables"].items()},
        "ignored_statement_types": result["ignored_statement_types"],
        "warnings": result["warnings"],
        "executed_sql": result["executed_sql"],
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
