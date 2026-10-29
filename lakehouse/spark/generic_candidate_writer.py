"""Fail-closed dispatcher from generic routing metadata to governed candidates."""

from __future__ import annotations

import argparse
import json

def _prepare_file_candidate(**kwargs):
    from file_candidate_writer import prepare_file_candidate

    return prepare_file_candidate(**kwargs)


def _prepare_database_candidate(**kwargs):
    from database_candidate_writer import prepare_database_candidate

    return prepare_database_candidate(**kwargs)


def prepare_generic_candidate(
    *, source_kind: str, input_path: str | None, manifest_path: str | None,
    decision_file: str, run_id: str
):
    normalized = str(source_kind or "").strip().lower()
    if normalized == "file":
        if not input_path:
            raise RuntimeError("Generic file candidate requires input_path")
        return _prepare_file_candidate(
            input_path=input_path, decision_file=decision_file, run_id=run_id
        )
    if normalized == "database":
        if not manifest_path:
            raise RuntimeError("Generic database candidate requires manifest_path")
        return _prepare_database_candidate(
            manifest_path=manifest_path, decision_file=decision_file, run_id=run_id
        )
    raise RuntimeError(
        f"NOT_CONFIGURED: source kind {normalized!r} has no governed candidate writer"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-kind", required=True, choices=("file", "database"))
    parser.add_argument("--input")
    parser.add_argument("--manifest")
    parser.add_argument("--decision-file", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    state = prepare_generic_candidate(
        source_kind=args.source_kind,
        input_path=args.input,
        manifest_path=args.manifest,
        decision_file=args.decision_file,
        run_id=args.run_id,
    )
    print(json.dumps(state.as_payload(), ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
