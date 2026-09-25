# Local (non-Docker) workflow. Assumes a Postgres warehouse reachable with the
# defaults in dbt/profiles.yml, or WAREHOUSE_* / WAREHOUSE_DSN overrides.
PY ?= python
DBT ?= dbt
export PYTHONPATH := $(CURDIR)
export DBT_PROFILES_DIR := $(CURDIR)/dbt

.PHONY: extract load dbt test all
extract:
	$(PY) -m include.ingestion.cli extract

load: extract
	$(PY) -m include.ingestion.cli load-reference
	$(PY) -m include.ingestion.cli load-orders --start 2016-09-01 --end 2018-11-01 --monthly

dbt:
	cd dbt && $(DBT) build

test:
	$(PY) -m pytest -q tests

all: load dbt test

.PHONY: stream
# Streaming demo on the file source: two runs, simulated crash replay, verification.
stream:
	PY=$(PY) bash streaming/run_demo.sh
