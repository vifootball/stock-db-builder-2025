import os
import yaml
import psycopg2

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