JAVA_HOME ?= $(CURDIR)/.tools/java
export JAVA_HOME
export PATH := $(JAVA_HOME)/bin:$(PATH)
export SPARK_LOCAL_IP := 127.0.0.1
export HOME := $(CURDIR)/.home

.PHONY: install demo test lint
install:
	mkdir -p .home .cache/ivy .cache/spark-tmp
	.venv/bin/pip install -e '.[dev]'

demo:
	mkdir -p .home .cache/ivy .cache/spark-tmp
	.venv/bin/python -m construction_data_platform.cli demo --root data/demo --scenario golden

test:
	mkdir -p .home .cache/ivy .cache/spark-tmp
	.venv/bin/pytest

lint:
	.venv/bin/ruff check src tests
