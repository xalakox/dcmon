SHELL := /bin/bash

.PHONY: run
run:
	uv run --script dcmon.py

.PHONY: test
test:
	python3 ./test_dcmon.py

.PHONY: lint
lint:
	uv run --with ruff --no-project ruff check dcmon.py test_dcmon.py

.PHONY: clean
clean:
	rm -rf __pycache__ .ruff_cache
