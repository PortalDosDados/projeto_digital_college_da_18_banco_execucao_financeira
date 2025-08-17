import psycopg2
from psycopg2 import OperationalError

def criar_conexao(db_name, db_user, db_password="", db_host="localhost", db_port="5432"):
    try:
        conexao = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        print("Conexão estabelecida com sucesso!")
        return conexao
    except OperationalError as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None

if __name__ == "__main__":
    conexao = criar_conexao(
        db_name="efd_trb_unid01",
        db_user="postgres",
        db_password="",  # senha vazia pois o banco não exige
        db_host="localhost",
        db_port="5432"
    )

    if conexao:
        cursor = conexao.cursor()
        cursor.execute("SELECT current_database();")
        db_atual = cursor.fetchone()
        print(f"Banco de dados conectado")

        cursor.close()
        conexao.close()
