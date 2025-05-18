## Requirements / Setup
This project runs PySpark on a Linux machine. You will need:

- Python 3.11+
- Java 11 SDK
- Poetry (for managing dependencies and virtual environments)
- Pre-commit (for linting and testing before each commit)

Install Java 11 SDK - run the following commands:

`sudo apt update`

`sudo apt install openjdk-11-jdk`


Set up the Python environment - Make sure Poetry is installed, then run:

`cd /path/to/this/repo`

`poetry install`

This will install all runtime and development dependencies, including:

- pyspark
- pytest
- ruff
- pre-commit

# Pre-commit hooks

Pre-commit is used to automatically run linting (ruff) and testing (pytest) before each commit.

1. Ensure pre-commit is installed (it will be via Poetry)
2. Install the Git hooks:

`pre-commit install`

3. (Optional) Run pre-commit checks manually on all files:
`pre-commit run --all-files`

This will:

- Automatically fix simple linting issues with ruff

- Run pytest to ensure your code passes all tests

Run the application:

`PYTHONPATH=src poetry run python src/de_challenge/app.py`


# Running Tests

Pytest is used for unit testing.

Run all tests:

`pytest`

Run a specific test file (only one had been included for the purposes of this challenge)

`pytest tests/test_triangle.py`

Show print/log output during tests

`pytest -s`

# Linting

Ruff is used for linting and import sorting.

- Run manually:

`ruff check .`

Automatically fix issues:

`ruff check --fix .`
