# ETL

### Caso:
Serão criados dois bancos, o banco fonte e o banco alvo, aalém disso uma API que fará a ponte entre o banco fonte e o script ETL

O script recebe uma data e com ela consulta a API obtendo os campos `wind_speed ` e `power` do banco fonte, 
serão feitas agregações de média, mínimo, máximo e desvio padrão com o pandas e os resultados salvos irão para o banco alvo

## Preparação do ambiente

```
python -m venv env
source env/bin/activate
pip install -r requirements.txt
```

## Bancos de dados

### Banco Fonte

No diretório `Base` alterar o .env para os acessos do seu banco e chave de segurança

Em `/Base/Schema` pode executar o script para criação do banco e tabelas
```
python /Base/Schema/schema.py

```

Pode gerar dados aleatórios para teste
```
python /Base/Schema/generate.py
```

### Banco Alvo

No diretório `Target` alterar o .env para os acessos do seu banco e da API

Em `/Target/Schema` pode executar o script para criação do banco e tabelas
```
python /Target/Schema/schema.py
```

## API

Esta foi feita para servir de ponte entre o banco de origem para o ETL

Por segurança antes de usar deve definir os domínios permitidos no CORS (`/Base/API/main.py`) e uma chave no .env (`/Base`) para servir para o gerador de tokens

### Inicializar

No diretório Base/API

```
uvicorn main:app --reload
```

### Endpoints (Exemplos)

- http://127.0.0.1:8007/generate-token
- http://127.0.0.1:8007/get-data-fields/?from_date=2024-11-09&to_date=2024-11-10&token=eyJhbGciOiJIUzI1NiIsInR5i7dfg

Para limitar os campos retornados no `/get-data-fields` deve passar o campo `fields` no header, todos separados por vírgulas, cada um sendo o nome de uma coluna do banco de origem  

## Processamento ETL

Ao executar só é exigida uma data para basear os dados, se quiser outros campos além do `wind_speed ` e `power`, ou outras funções matemáticas é só alterar o inicializador `__main__` do arquivo `etl.py`, neste tem listas alteráveis para essas definições

```
python /Target/Scripts/main.py --date <yyyy-mm-dd>
```
