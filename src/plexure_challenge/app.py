"""
Processes shape data using PySpark.

Loads shapes from JSON, maps each to a class (Triangle, Circle, Rectangle),
calculates areas, and prints the total.

Functions:
- initialise_shape_class: Picks the right shape class.
- compute_area: Builds shape and returns area.
- process_shapes: Loads data, maps areas, logs result.

Assumes each row has a 'type' and valid dimensions. Uses RDDs for processing.
"""

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, lower

from plexure_challenge.shapes import Triangle, Circle, Rectangle, Shape
from plexure_challenge.logging_utils import logger


def initialise_shape_class(shape_type: str, row: Row) -> Shape:
    """
    Takes the shape type and row, and returns the right Shape object.
    Args:
        shape_type (str): The name of the shape, already lowercase.
        row (Row): A Spark Row with the shape's attributes.
   
     Returns:
        Shape:  Triangle, Circle, or Rectangle instance.
    
    Raises:
        ValueError: If an invalid or undefined shape type is provided.
    """
    if shape_type == "triangle":
        return Triangle(row)
    elif shape_type == "circle":
        return Circle(row)
    elif shape_type == "rectangle":
        return Rectangle(row)
    else:
        raise ValueError(f"Invalid shape type: {shape_type}")


def compute_area(row: Row) -> float: 
    """
     Takes a Row, determines what shape it is, and returns the area.
        Args:
            row (Row): A Spark Row with shape data.
        
        Returns:
            float: Area of the shape, or None if something goes wrong.
        
        Logs:
            Any errors during shape setup or area calculation.
    """
    shape_type = row.type.lower()
    try:
        shape = initialise_shape_class(shape_type, row)
        return shape.calculate_area()
    except Exception as e:
        logger.error(f"Error computing area for row {row}: {e}")
        return None


## Using lower()
# All shape types in the dataset are already lowercase.
# I’ve kept the line: df = df.withColumn("type", lower(col("type"))) as a safeguard —
# assuming the shape "type" will either be spelled correctly or else caught as an invalid shape in initialise_shape()),
# but it might occasionally appear in a different casing (e.g., "Circle", "RECTANGLE").
# This just ensures consistency before checking against the list of valid shape types.


def process_shapes():
    """
    Reads shape data, calculates areas, and prints the total.

    Steps:
    1. Load shape data from JSON.
    2. Standardise shape type casing.
    3. Convert DataFrame to RDD and map each row to its area.
    4. Sum all valid areas and print the result.
    """
    spark = SparkSession.builder.appName("ShapeData").getOrCreate()
    shape_data = spark.read.json('src/plexure_challenge/data_lines.jsonl') 
    shape_data = shape_data.withColumn("type", lower(col("type")))
    areas_rdd = shape_data.rdd.map(compute_area)

    total_area = areas_rdd.sum()
    logger.info(f"Total area: {total_area:.2f}")
    

if __name__ == "__main__":
    process_shapes()