ARG PLATFORM=linux/amd64

FROM --platform=$PLATFORM python:3.10.4 as file_linters

WORKDIR /app

COPY pyproject.toml poetry.lock ./

RUN pip install --upgrade --progress-bar=off pip wheel \
    && pip install setuptools \
    && pip install poetry \
    && poetry export --without-hashes --only=file_linters --format=requirements.txt > requirements.txt \
    && pip install -r requirements.txt \
    && rm -rf /root/.cache/pip

COPY . /app