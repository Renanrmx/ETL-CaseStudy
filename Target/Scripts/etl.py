import os
import argparse
import httpx
from dotenv import load_dotenv, find_dotenv
import pandas as pd
import sqlalchemy.orm
from sqlalchemy import create_engine


load_dotenv(find_dotenv("../.env"))

USER = os.getenv("BASE_USER")
PASSWORD = os.getenv("BASE_PASSWORD")
HOST = os.getenv("BASE_HOST")
PORT = os.getenv("BASE_PORT")
DATABASE = os.getenv("BASE_DATABASE")
API_HOST = os.getenv("API_HOST")
API_PORT = os.getenv("API_PORT")

Base = sqlalchemy.orm.declarative_base()
engine = create_engine(f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}')


def get_data_from_source(date: str, fields: list[str]) -> pd.DataFrame | None:
    print("Obtendo dados da API...")

    try:
        base_url = f"http://{API_HOST}:{API_PORT}"
        token_response = httpx.get(f"{base_url}/generate-token")

        if token_response.status_code == 200:
            token_data = token_response.json()

            headers = {'fields': ", ".join(fields)}
            url = f"{base_url}/get-data-fields/?from_date={date}&to_date={date}&token={token_data['access_token']}"

            source_response = httpx.get(url, headers=headers)
            source_response.raise_for_status()

            data = source_response.json()
            df = pd.DataFrame(data)

            if df.empty:
                raise SystemExit("Erro: Dados inválidos da API")

            print("Dados da API obtidos com sucesso")

            return df

        else:
            print(f"Erro na requisição: {token_response.status_code}")

    except httpx.HTTPError as exc:
        print(f"Falha HTTP: {exc.request.url} - {exc}")
        raise SystemExit("Erro: Não foi possível obter os dados da API")


def set_data_to_target(df_operations: pd.DataFrame):
    print("Salvando os dados processados no banco...")

    Base.metadata.create_all(bind=engine)
    df_signals = pd.read_sql_table("signal", con=engine)

    df_melted = pd.melt(
        df_operations,
        id_vars=["timestamp", "operation"],
        value_vars=["wind_speed", "power"],
        var_name="name",
    )

    with pd.option_context("display.max_rows", None, "display.max_columns", None):
        df_melted_merged = df_melted.merge(
            df_signals,
            how="left",
            on=["name", "operation"],
        )

        df_melted_merged.rename(columns={"id": "signal_id"}, inplace=True)
        df_melted_merged.drop(["name", "operation"], axis=1, inplace=True)
        df_melted_merged.sort_values(by=["timestamp", "signal_id"], inplace=True)

    try:
        df_melted_merged.to_sql("data", con=engine, if_exists="append", index=False)
        print("Dados salvos com sucesso")

    except Exception as exc:
        print(exc)


def transform_data(source: pd.DataFrame, operations: list[str]) -> pd.DataFrame:
    print("Processando os dados com as operações definidas...")

    source["timestamp"] = pd.to_datetime(source["timestamp"])
    df_operations = pd.DataFrame()

    for operation in operations:
        df_operations_serie = (
            source.groupby(pd.Grouper(key="timestamp", freq="10min", origin="start"))
            .agg(operation)
            .reset_index()
        )

        df_operations_serie["operation"] = operation

        with pd.option_context("display.max_rows", None, "display.max_columns", None):
            print(df_operations_serie)

        df_operations = pd.concat([df_operations, df_operations_serie], ignore_index=True)

    with pd.option_context("display.max_rows", None, "display.max_columns", None):
        print(df_operations)

    print("Processos concluídos")

    return df_operations


def parse_input_date() -> str:
    parser = argparse.ArgumentParser(
        description="Script ETL",
    )
    parser.add_argument(
        "-date",
        "--date",
        required=True,
        type=str,
        help="data a ser processada (yyyy-mm-dd)",
    )
    args = parser.parse_args()
    date = args.date

    print(f"Escreva a data desejada: {date}")

    return date


if __name__ == "__main__":
    base_fields = ["wind_speed", "power", "timestamp"]
    etl_operations = ["std", "min", "max", "mean"]

    input_date = parse_input_date()
    df_source = get_data_from_source(input_date, fields=base_fields)
    df_set = transform_data(df_source, operations=etl_operations)
    set_data_to_target(df_set)
