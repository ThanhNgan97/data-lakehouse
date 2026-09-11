# -*- coding: utf-8 -*-

from pathlib import Path
import sys

import pytest


SPARK_DIR = Path(__file__).resolve().parent
REPO_ROOT = SPARK_DIR.parent.parent

if str(SPARK_DIR) not in sys.path:
    sys.path.insert(0, str(SPARK_DIR))

from sql_dump_parser import parse_mysql_dump_file, parse_mysql_dump_text


def test_project_mysql_dump():
    dump_path = REPO_ROOT / "mysql-init" / "01_init_kpi_tables.sql"

    result = parse_mysql_dump_file(dump_path)

    assert result["database"] == "cusc_kpi_operational"
    assert result["tables"]["don_vi"]["row_count"] == 6
    assert result["tables"]["muc_tieu_kpi"]["row_count"] == 34
    assert result["tables"]["ket_qua_danh_gia"]["row_count"] == 34
    assert result["warnings"] == []
    assert result["executed_sql"] is False


def test_dangerous_statements_are_ignored_not_executed():
    sql = """
    CREATE DATABASE IF NOT EXISTS test_kpi;
    USE test_kpi;

    DROP DATABASE something_important;
    CREATE USER 'evil'@'%' IDENTIFIED BY 'bad';
    GRANT ALL PRIVILEGES ON *.* TO 'evil'@'%';
    DELETE FROM ket_qua_danh_gia;

    CREATE TABLE don_vi (
        ma_don_vi VARCHAR(20),
        ten_don_vi VARCHAR(255)
    );

    CREATE TABLE muc_tieu_kpi (
        ma_chi_tieu VARCHAR(20),
        ma_don_vi VARCHAR(20),
        noi_dung VARCHAR(255),
        dinh_ky VARCHAR(50)
    );

    CREATE TABLE ket_qua_danh_gia (
        ma_chi_tieu VARCHAR(20),
        ma_don_vi VARCHAR(20),
        quy_danh_gia VARCHAR(20),
        muc_dang_ky VARCHAR(50),
        muc_dat VARCHAR(50),
        ket_qua_he_thong VARCHAR(50),
        nguyen_nhan TEXT,
        updated_at DATETIME
    );

    INSERT INTO don_vi
        (ma_don_vi, ten_don_vi)
    VALUES
        ('HT', 'Hạ tầng');

    INSERT INTO muc_tieu_kpi
        (ma_chi_tieu, ma_don_vi, noi_dung, dinh_ky)
    VALUES
        ('HT-MT01', 'HT', 'Kiểm thử SQL dump', 'Quý');

    INSERT INTO ket_qua_danh_gia
        (
            ma_chi_tieu,
            ma_don_vi,
            quy_danh_gia,
            muc_dang_ky,
            muc_dat,
            ket_qua_he_thong,
            nguyen_nhan,
            updated_at
        )
    VALUES
        (
            'HT-MT01',
            'HT',
            'Q4/2026',
            '100%',
            '100%',
            'ĐẠT',
            'Không có',
            '2026-09-11 10:00:00'
        );
    """

    result = parse_mysql_dump_text(sql)

    assert result["executed_sql"] is False
    assert result["tables"]["don_vi"]["row_count"] == 1
    assert result["tables"]["muc_tieu_kpi"]["row_count"] == 1
    assert result["tables"]["ket_qua_danh_gia"]["row_count"] == 1

    ignored = result["ignored_statement_types"]
    assert ignored["DROP_DATABASE"] == 1
    assert ignored["CREATE_USER"] == 1
    assert ignored["GRANT"] == 1
    assert ignored["DELETE"] == 1


def test_missing_required_kpi_table_fails():
    sql = """
    CREATE DATABASE test_missing;
    USE test_missing;

    CREATE TABLE don_vi (
        ma_don_vi VARCHAR(20)
    );

    CREATE TABLE muc_tieu_kpi (
        ma_chi_tieu VARCHAR(20),
        ma_don_vi VARCHAR(20)
    );

    INSERT INTO don_vi (ma_don_vi)
    VALUES ('HT');

    INSERT INTO muc_tieu_kpi (ma_chi_tieu, ma_don_vi)
    VALUES ('HT-MT01', 'HT');
    """

    with pytest.raises(
        ValueError,
        match="ket_qua_danh_gia",
    ):
        parse_mysql_dump_text(sql)


def test_semicolon_and_vietnamese_text_inside_insert():
    sql = """
    CREATE DATABASE test_unicode;
    USE test_unicode;

    CREATE TABLE don_vi (
        ma_don_vi VARCHAR(20),
        ten_don_vi VARCHAR(255)
    );

    CREATE TABLE muc_tieu_kpi (
        ma_chi_tieu VARCHAR(20),
        ma_don_vi VARCHAR(20),
        noi_dung VARCHAR(255),
        dinh_ky VARCHAR(50)
    );

    CREATE TABLE ket_qua_danh_gia (
        ma_chi_tieu VARCHAR(20),
        ma_don_vi VARCHAR(20),
        quy_danh_gia VARCHAR(20),
        muc_dang_ky VARCHAR(50),
        muc_dat VARCHAR(50),
        ket_qua_he_thong VARCHAR(50),
        nguyen_nhan TEXT,
        updated_at DATETIME
    );

    INSERT INTO don_vi
        (ma_don_vi, ten_don_vi)
    VALUES
        ('HT', 'Hạ tầng kỹ thuật');

    INSERT INTO muc_tieu_kpi
        (ma_chi_tieu, ma_don_vi, noi_dung, dinh_ky)
    VALUES
        ('HT-MT01', 'HT', 'Đảm bảo hệ thống hoạt động', 'Quý');

    INSERT INTO ket_qua_danh_gia
        (
            ma_chi_tieu,
            ma_don_vi,
            quy_danh_gia,
            muc_dang_ky,
            muc_dat,
            ket_qua_he_thong,
            nguyen_nhan,
            updated_at
        )
    VALUES
        (
            'HT-MT01',
            'HT',
            'Q4/2026',
            '100%',
            '99,5%',
            'KHÔNG ĐẠT',
            'Không có; dữ liệu vẫn an toàn',
            '2026-09-11 10:00:00'
        );
    """

    result = parse_mysql_dump_text(sql)

    rows = result["tables"]["ket_qua_danh_gia"]["rows"]

    assert len(rows) == 1
    assert rows[0]["ket_qua_he_thong"] == "KHÔNG ĐẠT"
    assert rows[0]["muc_dat"] == "99,5%"
    assert rows[0]["nguyen_nhan"] == "Không có; dữ liệu vẫn an toàn"
    assert result["executed_sql"] is False
