import os
import random
import sqlalchemy
from dotenv import load_dotenv
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, Column, Numeric, Integer, DateTime, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta


dotenv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env')
load_dotenv(dotenv_path)

# Dados de acesso
USER = os.getenv("BASE_USER")
PASSWORD = os.getenv("BASE_PASSWORD")
HOST = os.getenv("BASE_HOST")
PORT = os.getenv("BASE_PORT")
DATABASE = os.getenv("BASE_DATABASE")

DATABASE_URL = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"

Base = sqlalchemy.orm.declarative_base()


class Data(Base):
    __tablename__ = 'data'

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False)
    wind_speed = Column(Numeric, nullable=False)
    power = Column(Numeric, nullable=False)
    ambient_temperature = Column(Numeric, nullable=False)


engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()


def populate_data_table(start_date: datetime, days: int = 10):
    """
    Popula a tabela `data` com dados aleatórios de frequência 1-minutal em um intervalo de 10 dias.

    Args:
        start_date (datetime): Data inicial para os registros.
        days (int): Intervalo de dias para geração de dados.
    """

    total_minutes = days * 24 * 60      # 10 dias x 24 horas x 60 minutos

    for i in range(total_minutes):
        timestamp = start_date + timedelta(minutes=i)
        wind_speed = round(random.uniform(0, 15), 2)            # Velocidade do vento entre 0 e 15 m/s
        power = round(random.uniform(0, 1000), 2)               # Potência entre 0 e 1000 kW
        temperature = round(random.uniform(-5, 40), 2)             # Temperatura entre -5 e 40 °C

        data_row = Data(
            timestamp=timestamp,
            wind_speed=wind_speed,
            power=power,
            ambient_temperature=temperature
        )

        session.add(data_row)

    session.commit()

    print("Registros inseridos com sucesso!")


if __name__ == "__main__":
    start_date = datetime.now() - timedelta(days=10)
    populate_data_table(start_date=start_date, days=10)
