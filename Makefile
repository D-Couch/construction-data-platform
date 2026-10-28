JAVA_HOME ?= $(CURDIR)/.tools/java
export JAVA_HOME
export PATH := $(JAVA_HOME)/bin:$(PATH)
export SPARK_LOCAL_IP := 127.0.0.1
export HOME := $(CURDIR)/.home
export PYSPARK_SUBMIT_ARGS ?= --driver-memory 4g pyspark-shell

.PHONY: install demo saas-demo test lint recovery-demo benchmark
install:
	mkdir -p .home .cache/ivy .cache/spark-tmp
	.venv/bin/pip install -e '.[dev]'

demo:
	mkdir -p .home .cache/ivy .cache/spark-tmp
	.venv/bin/python -m construction_data_platform.cli demo --root data/demo --scenario golden

saas-demo:
	mkdir -p .home .cache/ivy .cache/spark-tmp
	.venv/bin/python examples/saas_usage_billing/demo.py --root data/saas-demo

test:
	mkdir -p .home .cache/ivy .cache/spark-tmp
	.venv/bin/pytest

lint:
	.venv/bin/ruff check src tests

recovery-demo:
	@.venv/bin/python scripts/demo-recovery.py

benchmark:
	@.venv/bin/python scripts/benchmark-allocation-join.py $(BENCHMARK_ARGS)
