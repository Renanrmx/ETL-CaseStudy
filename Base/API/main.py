import os
import logging
import uvicorn
from typing import Optional
from pydantic import BaseModel, Field
from typing import Annotated
from fastapi import FastAPI, Request, HTTPException, Query, Depends
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse
import jwt
from datetime import date, datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import select, and_


logging.basicConfig(filename="base_api.log", level=logging.INFO)

dotenv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env')
load_dotenv(dotenv_path)

USER = os.getenv("BASE_USER")
PASSWORD = os.getenv("BASE_PASSWORD")
HOST = os.getenv("BASE_HOST")
PORT = os.getenv("BASE_PORT")
DATABASE = os.getenv("BASE_DATABASE")

SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 5
ALLOWED_ORIGINS = ["http://127.0.0.1"]  # TODO: Definir domínios permitidos

DATABASE_URL = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"


app = FastAPI(
    title="Base Middleware",
    docs_url=None,  # Docs ocultados por segurança
    redoc_url=None,
    openapi_url=None
)

app.add_middleware(
    CORSMiddleware,
    allow_credentials=False,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"]
)


def create_access_token(expires_delta: Optional[timedelta] = None):
    to_encode = {"system": "my_system_identifier"}
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=15))
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

    return encoded_jwt


async def validate_token(token: str):
    credentials_exception = HTTPException(
        status_code=401,
        detail="Token inválido ou expirado.",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        if payload.get("system") != "my_system_identifier":
            raise credentials_exception

    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expirado")

    except jwt.InvalidTokenError:
        raise credentials_exception


class DateRange(BaseModel):
    from_date: date = Field(...)
    to_date: date = Field(...)


def validate_date_range(
    from_date: Annotated[date, Query(...)],
    to_date: Annotated[date, Query(...)]
):
    if from_date > to_date:
        raise ValueError("A data inicial não pode ser maior que a final")

    return {"from_date": from_date, "to_date": to_date}


@app.middleware("http")
async def dispatch(request: Request, call_next):
    origin = request.client.host

    if origin not in [allowed.split('//')[1] for allowed in ALLOWED_ORIGINS]:
        return JSONResponse(
            status_code=403,
            content={"detail": "Acesso não autorizado."}
        )

    response = await call_next(request)

    response.headers['Access-Control-Allow-Origin'] = ", ".join(ALLOWED_ORIGINS)

    return response


async def get_from_base(data_range: DateRange, headers: dict):
    engine = create_engine(DATABASE_URL)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = SessionLocal()
    metadata = MetaData()
    data_table = Table("data", metadata, autoload_with=engine)

    try:
        date_filter = and_(
            data_table.c.timestamp >= data_range.from_date,
            data_table.c.timestamp < data_range.to_date + timedelta(days=1)
        )

        if "fields" in headers:
            columns_to_select = [field.strip() for field in headers["fields"].split(",")]
            columns = [data_table.c[col] for col in columns_to_select if col in data_table.c]

            query = session.execute(
                select(*columns).where(
                    date_filter
                )
            )
            return query.mappings().all()

        else:
            query = session.execute(
                data_table.select().where(
                    date_filter
                ).execution_options(show_keys=True)
            )
            return query.mappings().all()

    except SQLAlchemyError as e:
        return {"error": str(e)}

    finally:
        session.close()


@app.get("/generate-token")
async def generate_token():
    access_token = create_access_token(expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    return {"access_token": access_token, "token_type": "bearer"}


@app.get("/get-data-fields/")
async def get_base_data_fields(
        request: Request,
        data_range: dict = Depends(validate_date_range),
        token: str = Depends(validate_token)):
    headers = dict(request.headers)
    data = DateRange.model_validate(data_range)

    return await get_from_base(data, headers)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8007, log_level="debug")
