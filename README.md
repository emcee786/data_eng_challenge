# Requirements / Setup
This project runs PySpark on a Linux machine. You will need:

Python 3.11+
Java 11 SDK
Poetry (for managing dependencies and virtual environments)

Install Java 11 SDK - run the following commands:

`sudo apt update`

`sudo apt install openjdk-11-jdk`


Set up the Python environment - Make sure Poetry is installed, then run:

`cd /path/to/this/repo`

`poetry install`


Run the application:

`PYTHONPATH=src poetry run python src/plexure_challenge/app.py`


RUNNING TESTS

Pytest is used for unit testing.

Run all tests:

`pytest`

Run a specific test file (only one had been included for the purposes of this challenge)

`pytest tests/test_triangle.py`

Show print/log output during tests

`pytest -s`