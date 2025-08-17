import psycopg2
from psycopg2 import OperationalError, DatabaseError
from typing import Any, List, Optional, Tuple

# Configuração para o PostgreSQL
DB_CONFIG = {
    "host": "localhost",
    "dbname": "da_18_trabalho_exec_financeira",
    "user": "postgres",
    "password": "12345",
    "sslmode": "disable"  # troque para "require" se o servidor exigir SSL
}


def create_connection() -> Optional[psycopg2.extensions.connection]:
    """
    Cria e retorna uma conexão com o banco.
    Retorna None se houver falha na conexão.
    """
    try:
        return psycopg2.connect(**DB_CONFIG) # type: ignore
    except OperationalError:
        return None


def execute_query(query: str, params: Optional[Tuple[Any]] = None) -> bool:
    """
    Executa comandos INSERT, UPDATE ou DELETE.
    Retorna True se a execução foi bem-sucedida, False caso contrário.
    """
    conn = create_connection()
    if not conn:
        return False

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
        return True
    except DatabaseError:
        return False
    finally:
        conn.close()


def fetch_query(query: str, params: Optional[Tuple[Any]] = None) -> Tuple[List[Tuple[Any]], List[str]]:
    """
    Executa SELECT e retorna os resultados e os nomes das colunas.
    Retorna ([], []) em caso de falha.
    """
    conn = create_connection()
    if not conn:
        return [], []

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                colnames = [desc[0] for desc in cur.description]
                result = cur.fetchall()
        return result, colnames
    except DatabaseError:
        return [], []
    finally:
        conn.close()
