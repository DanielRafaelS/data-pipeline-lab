FROM prefecthq/prefect:3.6.16-python3.11

WORKDIR /app

COPY pyproject.toml .
COPY etl ./etl

RUN pip install --upgrade pip \
    && pip install .

CMD ["prefect", "worker", "start", "--type", "process", "--pool", "local-pool"]
