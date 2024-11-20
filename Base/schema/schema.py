import os
from dotenv import load_dotenv
import sqlalchemy.orm
from sqlalchemy import create_engine, Column, text, Integer, Numeric, DateTime


dotenv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env')
load_dotenv(dotenv_path)

# Dados de acesso
USER = os.getenv("BASE_USER")
PASSWORD = os.getenv("BASE_PASSWORD")
HOST = os.getenv("BASE_HOST")
PORT = os.getenv("BASE_PORT")
DATABASE = os.getenv("BASE_DATABASE")

Base = sqlalchemy.orm.declarative_base()


class Data(Base):
    __tablename__ = 'data'

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False)
    wind_speed = Column(Numeric, nullable=False)
    power = Column(Numeric, nullable=False)
    ambient_temperature = Column(Numeric, nullable=False)


def set_tables():
    engine = create_engine(f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}')

    Base.metadata.create_all(engine)
    print(f"Esquema do banco '{DATABASE}' definido com sucesso")


def create_database():
    engine = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/postgres", isolation_level="AUTOCOMMIT")

    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT 1 FROM pg_database WHERE datname = '{DATABASE}';"))

        if not result.fetchone():
            conn.execute(text(f"CREATE DATABASE {DATABASE};"))
            print(f"Banco de dados '{DATABASE}' criado com sucesso")


if __name__ == "__main__":
    try:
        create_database()
        set_tables()

    except Exception as e:
        print(f"Erro ao definir o banco de dados: {e}")
