FROM mageai/mageai:latest

RUN pip3 install --no-cache-dir \
    dbt-postgres \
    psycopg2-binary