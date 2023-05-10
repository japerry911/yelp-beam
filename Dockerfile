ARG PLATFORM=linux/amd64
ARG PYTHONPATH=/app/src
ARG VIRTUAL_ENV=/opt/venv

# ---FILE_LINTERS stage---
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

# ---SKY_BEAM stage---
FROM --platform=$PLATFORM python:3.10.4 as sky_beam

ARG PYTHONPATH
ARG VIRTUAL_ENV

ENV PYTHONATH=$PYTHONPATH
ENV VIRTUAL_ENV=$VIRTUAL_ENV
ENV GOOGLE_CLOUD_PROJECT=sky-beam

WORKDIR /app

COPY pyproject.toml poetry.lock ./

RUN pip install --upgrade --progress-bar=off pip wheel \
    && pip install setuptools \
    && pip install poetry \
    && poetry export --without-hashes --format requirements.txt > requirements.txt \
    && pip install -r requirements.txt \
    && rm -rf /root/.cache/pip

COPY . ./

WORKDIR /app/src
