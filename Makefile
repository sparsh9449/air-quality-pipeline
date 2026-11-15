.PHONY: setup up down test lint poll poll-publish topics stream-check

VENV := .venv
PY := $(VENV)/bin/python

setup:
	python3 -m venv $(VENV)
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -r requirements.txt ruff pytest

up:
	docker compose up -d

down:
	docker compose down

test:
	$(PY) -m pytest

lint:
	$(PY) -m ruff check .

poll:
	$(PY) -m ingestion.poller --loop

poll-publish:
	$(PY) -m ingestion.poller --publish

topics:
	$(PY) -m streaming.admin

stream-check:
	$(PY) -m scripts.roundtrip
