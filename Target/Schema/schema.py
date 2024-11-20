import os
from dotenv import load_dotenv, find_dotenv
import sqlalchemy.orm
from sqlalchemy import create_engine, Column, ForeignKey, text, Numeric, String, TIMESTAMP, Integer
from sqlalchemy.orm import relationship


load_dotenv(find_dotenv("../.env"))

USER = os.getenv("BASE_USER")
PASSWORD = os.getenv("BASE_PASSWORD")
HOST = os.getenv("BASE_HOST")
PORT = os.getenv("BASE_PORT")
DATABASE = os.getenv("BASE_DATABASE")

Base = sqlalchemy.orm.declarative_base()


class Signal(Base):
    __tablename__ = 'signal'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    operation = Column(String)

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "operation": self.operation,
        }

    data = relationship("Data", back_populates="signal", cascade="all, delete-orphan")


class Data(Base):
    __tablename__ = 'data'

    id = Column(Integer, primary_key=True, autoincrement=True)
    signal_id = Column(Integer, ForeignKey('signal.id', ondelete="CASCADE"), nullable=False)
    timestamp = Column(TIMESTAMP, nullable=False)
    value = Column(Numeric, nullable=False)

    signal = relationship("Signal", back_populates="data")


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


def populate_signals_table(fields, operations):
    """
    A tabela signals precisa ser previamente populada antes dos processos ETL
    Ela tem a combinação de todos os campos processados com as operações existentes
    """

    engine = create_engine(f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}')

    print("Populando a tabela signal...")

    Base.metadata.create_all(bind=engine)

    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM signal"))
            count = result.scalar()

        if count == 0:
            signals_data = []
            for idx, (name, operation) in enumerate(
                    [(var, agg) for var in fields for agg in operations], start=1
            ):
                signals_data.append({"id": idx, "name": name, "operation": operation})

            with engine.begin() as conn:
                conn.execute(Signal.__table__.insert(), signals_data)

            print("Tabela signal populada com sucesso")

        else:
            print("Tabela signal não está vazia, não foi populada com novos dados")

    except Exception as exc:
        print(f"Erro ao popular tabela signal: {exc}")


if __name__ == "__main__":
    try:
        create_database()
        set_tables()

        base_fields = ["timestamp", "wind_speed", "power", "ambient_temprature"]
        etl_operations = ["std", "min", "max", "mean"]
        populate_signals_table(base_fields, etl_operations)

    except Exception as e:
        print(f"Erro ao definir o banco de dados: {e}")
