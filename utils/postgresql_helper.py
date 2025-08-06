import os
import yaml
import psycopg2
import pandas as pd

def load_db_config(connection_name: str, config_path="config/config.yaml") -> dict:
    """Load Config File from YAML"""
    try:
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)
    except Exception as e:
        raise RuntimeError(f"[ERROR] Failed to load config file: {e}")

    profiles = config.get("postgresql_db_profiles", {})
    if connection_name not in profiles:
        raise ValueError(f"[ERROR] Profile '{connection_name}' not found in config.")
    
    return profiles[connection_name]


def connect_db(connection_name: str) -> psycopg2.extensions.connection:
    """
    Connect to PostgreSQL Database using YAML Config File
    load_db_config -> db 연결
    """
    conn = None

    try:
        db_params = load_db_config(connection_name)
        conn = psycopg2.connect(**db_params)
        print(f"[INFO] Successfully connected to '{connection_name}'.")

    except psycopg2.OperationalError as e:
        print(f"[ERROR] Failed to connect to '{connection_name}': {e}")
    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")

    return conn


def list_databases(connection_name: str) -> list:
    """PostgreSQL 서버에 존재하는 모든 데이터베이스 이름 조회"""
    """Postgresql 구조: Database - Schema - Table"""
    conn = connect_db(connection_name) # 아무 database로 연결해도 모든 database 조회됨
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
    databases = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return databases


def list_schemas(connection_name: str):
    """config.yaml에 정의된 DB에 존재하는 스키마 목록 조회"""
    conn = connect_db(connection_name)
    cur = conn.cursor()
    cur.execute("""
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name NOT IN (
            'pg_catalog', 'information_schema', 'pg_toast'
        )
        AND schema_name NOT LIKE 'pg_toast_temp_%'
        AND schema_name NOT LIKE 'pg_temp_%';
    """)
    schemas = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return schemas


def list_tables(connection_name: str, schema: str):
    """지정된 스키마 내의 테이블 목록 조회"""
    conn = connect_db(connection_name)
    cur = conn.cursor()
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = %s AND table_type = 'BASE TABLE';
    """, (schema,))
    tables = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return tables


def list_columns(connection_name: str, schema: str, table: str):
    """지정된 테이블의 컬럼 목록 조회"""
    conn = connect_db(connection_name)
    cur = conn.cursor()
    cur.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = %s AND table_name = %s;
    """, (schema, table))
    columns = cur.fetchall()  # [(col1, type1), (col2, type2), ...]
    cur.close()
    conn.close()
    return columns


# db에 따라 시간 오래 걸릴 수 있음
def summarize_tables_in_schema(connection_name: str, schema: str = 'public') -> pd.DataFrame:
    """특정 스키마 내 모든 테이블의 요약 정보 출력"""
    conn = connect_db(connection_name)
    cur = conn.cursor()

    # 모든 테이블 가져오기
    cur.execute(f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
        ORDER BY table_name;
    """, (schema,))
    tables = [row[0] for row in cur.fetchall()]

    summary = []
    for i, table in enumerate(tables, 1):
        print(f"[{i}/{len(tables)}] Processing table: {table} ...")

        # row, col count
        cur.execute(f"SELECT COUNT(*) FROM {schema}.{table};")
        row_count = cur.fetchone()[0]

        cur.execute(f"""
            SELECT COUNT(*),
                BOOL_OR(column_name IN ('created_at', 'updated_at')) AS has_timestamp_col
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s;
        """, (schema, table))
        col_count, has_timestamp_col = cur.fetchone()

        # 메타정보 조회
        cur.execute(f"""
            SELECT 
                pg_total_relation_size(oid) / 1024 / 1024 AS size_mb,
                EXISTS (
                    SELECT 1 FROM pg_index WHERE pg_index.indrelid = c.oid
                ) AS has_index,
                EXISTS (
                    SELECT 1 FROM pg_constraint WHERE conrelid = c.oid AND contype = 'p'
                ) AS has_primary_key,
                EXISTS (
                    SELECT 1 FROM pg_partitioned_table WHERE partrelid = c.oid
                ) AS is_partitioned
            FROM pg_class c
            WHERE relname = %s;
        """, (table,))
        meta = cur.fetchone()

        summary.append({
            'table_name': table,
            'row_count': row_count,
            'column_count': col_count,
            'size_mb': meta[0],
            'has_index': meta[1],
            'has_primary_key': meta[2],
            'is_partitioned': meta[3],
            'has_timestamp_col': has_timestamp_col,
        })

    cur.close()
    conn.close()

    return pd.DataFrame(summary)
