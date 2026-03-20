SHELL:=/usr/bin/env bash -O globstar
.SHELLFLAGS = -ec

.PHONY: build clean deep-clean format install lint python-pre-commit reinstall-poetry test

build:
	poetry build

clean:
	rm -rf dist
	rm -rf htmlcov
	rm -rf .coverage

deep-clean: clean
	rm -rf .venv

format:
	poetry run black src tests
	poetry run isort src tests

install:
	poetry install && \
	poetry run pre-commit install

lint:
	poetry run flake8 src tests
	poetry run black --check src tests
	poetry run isort --check-only src tests

python-pre-commit: lint
	poetry lock
	poetry check

reinstall-poetry:
	./.github/scripts/reinstall-poetry.sh

test: build
	poetry run python -m coverage run --data-file=.coverage/coverage -m pytest tests && \
	poetry run python -m coverage xml --data-file=.coverage/coverage -o .coverage/info.xml

%:
	@$(MAKE) -f /usr/local/share/eps/Mk/common.mk $@
