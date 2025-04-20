import re
import pytest
from pyspark.sql import Row
from src.plexure_challenge.shapes import Triangle

# I've added a suite of test cases for the Triangle shape only.
# In a full application, I would also include tests for the other shapes (Rectangle and Circle),
# as well as for the abstract base class and the functions in app.py.
# For this challenge, I've focused on Triangle to demonstrate my approach to testing,
# including validation, edge cases, and numeric input handling. 

def test_triangle_valid_initialisation():
    row = Row(base = 3, height = 4)
    triangle = Triangle(row)
    assert triangle.base == 3.0
    assert triangle.height == 4.0


def test_triangle_area():
    row = Row(base = 3, height = 4)
    triangle = Triangle(row)
    assert triangle.calculate_area() == 6.0


def test_triangle_initialises_with_numeric_strings():
    row = Row(base="3", height="4")
    triangle = Triangle(row)
    assert triangle.base == 3.0
    assert triangle.height == 4.0


def test_triangle_missing_field():
    row = Row(base=3) # missing height
    with pytest.raises(AttributeError):
        Triangle(row)


@pytest.mark.parametrize("base, height", [
    (0, 5),      
    (5, 0),       
    (-1, 5),      
    (5, -1),      
], ids=["zero base", "zero height", "negative base", "negative height"])


def test_triangle_invalid_dimensions(base, height):
    row=Row(base=base, height=height)
    with pytest.raises(ValueError, match=re.escape("Base and height must be greater than zero")):
        Triangle(row)


@pytest.mark.parametrize("base, height", [
    ("four", 5),
    (5, "three"),
    (None, 5),
    (5, None),
], ids=["base not numeric", "height not numeric", "base is None", "height is None"])


def test_triangle_non_numeric_input(base, height):
    row = Row(base=base, height=height)
    with pytest.raises((TypeError, ValueError)):
        Triangle(row)
