FROM python:3.9.1

RUN pip install --no-cache-dir pandas     

WORKDIR /docker_sql

COPY pipeline.py pipeline.py

ENTRYPOINT ["python","pipeline.py"]
