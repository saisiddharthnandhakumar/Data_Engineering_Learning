FROM python:3.9.1

RUN apt-get install wget
RUN pip install --no-cache-dir pandas sqlalchemy psycopg2

WORKDIR /docker_sql

COPY ingest_data.py ingest_data.py

ENTRYPOINT ["python","ingest_data.py"]
