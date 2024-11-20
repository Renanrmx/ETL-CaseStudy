# ETL

## Preparação do ambiente

```
pip install -r requirements.txt
```

## API

Esta foi feita para servir de ponte entre o banco de origem para o ETL

Por segurança antes de usar deve definir os domínios permitidos no CORS e uma chave no .env para servir para o gerador de tokens

### Inicializar
```
uvicorn main:app --reload
```

### Endpoints (Exemplos)

- http://127.0.0.1:8007/generate-token
- http://127.0.0.1:8007/get-data-fields/?from_date=2024-11-09&to_date=2024-11-10&token=eyJhbGciOiJIUzI1NiIsInR5i7dfg

Para limitar os campos retornados no ```/get-data-fields``` deve passar o campo ```fields``` no header, todos separados por vírgulas, cada um sendo o nome de uma coluna do banco de origem  
