import os
import yaml
import psycopg2
import logging
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


def create_database(connection_name, database_name):
    # connection_name: 기본 db가 존재하는 테스트용 연결을 쓴다
    conn = connect_db(connection_name)
    print(f"Database Connected for create_database({database_name})")

    # autocommit 설정
    conn.autocommit = True

    try:
        with conn.cursor() as cursor:
            # 데이터베이스 존재 여부 확인
            cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = %s;", (database_name,))
            exists = cursor.fetchone()

            # 데이터베이스가 없으면 생성
            if not exists:
                cursor.execute(f"CREATE DATABASE {database_name};")
                print(f"Database '{database_name}' created successfully.")
            else:
                print(f"Database '{database_name}' already exists.")

    except psycopg2.Error as e:
        print(f"An error occurred: {e}")

    finally:
        # 연결 닫기
        conn.close()

# Database - Schema - Table 구조인데, Schema를 명시하지 않으면 자동으로 'public' 스키마로 들어감
def copy_csv_file_to_db(csv_file_path: str, connection_name: str, table_name: str, create_table_sql_path: str, mode: str = "replace"):
    """
    단일 CSV 파일을 PostgreSQL에 업로드하는 함수.

    Args:
        csv_file_path (str): 업로드할 CSV 파일 경로
        table_name (str): 데이터를 저장할 테이블 이름
        create_table_query (str): 테이블 생성 쿼리문 (replace 모드에서만 사용)
        mode (str): 'replace' 또는 'append'
    """
    if mode not in ("replace", "append"):
        raise ValueError("[ERROR] mode must be either 'replace' or 'append'.")

    # SQL 파일 로드: replace 모드에서만 create table 쿼리문 로드
    if mode == "replace":
        if not os.path.isfile(create_table_sql_path):
            raise FileNotFoundError(f"[ERROR] SQL file not found: {create_table_sql_path}")
        with open(create_table_sql_path, "r") as file:
            create_table_query = file.read()

    conn = connect_db(connection_name)

    try:
        with conn:
            with conn.cursor() as cursor:

                if mode == "replace":
                    # (1) 기존 테이블 삭제 및 생성
                    logging.info(f"[{table_name}] Drop table if exists: Started")
                    cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
                    logging.info(f"[{table_name}] Drop table if exists: Completed")

                    logging.info(f"[{table_name}] Create table: Started")
                    cursor.execute(create_table_query)
                    logging.info(f"[{table_name}] Create table: Completed")

                elif mode == "append":
                    logging.info(f"[{table_name}] Append mode: Append to existing table '{table_name}'")

                # (2) CSV 데이터 업로드
                logging.info(f"[{table_name}] Uploading from '{csv_file_path}': Started")
                with open(csv_file_path, 'r') as f:
                    cursor.copy_expert(
                        f"COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER ','", f
                    )
                logging.info(f"[{table_name}] Uploading: Completed")

                # (3) 업로드된 행 수 확인
                cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
                row_count = cursor.fetchone()[0]
                logging.info(f"[{table_name}] Total rows after upload: {row_count}")

    except Exception as e:
        logging.info(f"[ERROR] Failed to load CSV into '{table_name}': {e}")
        conn.rollback()

    finally:
        conn.close()


import os
import logging

def copy_csv_files_to_db(
    csv_dir_path: str,
    connection_name: str,
    table_name: str,
    create_table_sql_path: str,
    mode: str = "replace",
):
    """
    폴더 내 모든 CSV를 PostgreSQL 테이블에 업로드.

    Args:
        csv_dir_path (str): CSV 파일들이 위치한 디렉터리
        connection_name (str): config.yaml에 정의된 DB 연결 이름
        table_name (str): 적재 대상 테이블명
        create_table_sql_path (str): 테이블 생성 SQL 파일 경로 (replace 모드에서 사용)
        mode (str): 'replace' | 'append'
    """
    if mode not in ("replace", "append"):
        raise ValueError("[ERROR] mode must be either 'replace' or 'append'.")

    # 업로드 대상 파일 수집
    filenames = sorted([f for f in os.listdir(csv_dir_path) if f.lower().endswith(".csv")])
    if not filenames:
        logging.warning(f"[{table_name}] No CSV files found in: {csv_dir_path}")
        return

    # SQL 로드 (replace일 때만 필요)
    create_table_query = None
    if mode == "replace":
        if not os.path.isfile(create_table_sql_path):
            raise FileNotFoundError(f"[ERROR] SQL file not found: {create_table_sql_path}")
        with open(create_table_sql_path, "r") as f:
            create_table_query = f.read()

    conn = connect_db(connection_name)

    try:
        with conn:
            with conn.cursor() as cursor:
                if mode == "replace":
                    logging.info(f"[{table_name}] Drop table if exists: Started")
                    cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
                    logging.info(f"[{table_name}] Drop table if exists: Completed")

                    logging.info(f"[{table_name}] Create table: Started")
                    cursor.execute(create_table_query)
                    logging.info(f"[{table_name}] Create table: Completed")
                else:
                    logging.info(f"[{table_name}] Append mode: Using existing table")

                total = len(filenames)
                for idx, fname in enumerate(filenames, start=1):
                    fpath = os.path.join(csv_dir_path, fname)
                    logging.info(f"[{table_name}] [{idx}/{total}] COPY from '{fpath}': Started")
                    with open(fpath, "r") as f:
                        cursor.copy_expert(
                            f"COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER ','",
                            f
                        )
                    logging.info(f"[{table_name}] [{idx}/{total}] COPY: Completed")

                    # 누적 로우 카운트 확인
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
                    row_count = cursor.fetchone()[0]
                    logging.info(f"[{table_name}] Rows after '{fname}': {row_count}")

    except Exception as e:
        logging.exception(f"[{table_name}] Failed to load CSVs from '{csv_dir_path}'")
        conn.rollback()
        raise
    finally:
        conn.close()

