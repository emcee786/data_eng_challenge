# Requirements / Setup

This code runs pyspark on a linux machine, you will require python and java11 sdk to build/run it

To install the jvav sdk you can run the following commands

`sudo apt update`

`sudo apt install openjdk-11-jdk`


To build the python environment run (you will need poetry installed)

`cd /path/to/this/repo`

`poetry install`


To run the base functionality of the app use the following command

`PYTHONPATH=src poetry run python src/plexure_challenge/app.py`
