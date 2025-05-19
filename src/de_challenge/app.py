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

from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col, lower

from de_challenge.logging_utils import logger
from de_challenge.shapes import Circle, Rectangle, Shape, Triangle


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
        #Invalid shape would break script. Should it log and continue?

# Moving error handling out of the Shape constructors and ensuring the script logs 
# errors rather than breaking, based on feedback.
# I believe there's a better way to handle this using more of PySpark's capabilities.

def validate_row_fields(row: Row, fields: list):
    """
    Validates that the required fields are present, castable to float, and greater than zero.
    
    Args:
        row (Row): A Spark Row.
        fields (list): List of required field names.
    
    Returns:
        bool: True if valid, False otherwise.
    """
    for field in fields:
        value = getattr(row, field, None)

        if value is None:
            logger.error(f"Validation Error - Missing field '{field}'."
                         f"Row contents: {row.asDict()}")
            return False
        
        try:
            value_float = float(value)
        except (TypeError, ValueError):
            logger.error(
                f"Type Error - Field '{field}' cannot be cast to float. (Got type: {type(value).__name__})"
                f"Row contents: {row.asDict()} "
                
            )
    
        if value_float <= 0:
            logger.error(
                f"Validation Error - Field '{field}' must be greater than zero. Got value: {value_float}. "
                f"Row contents: {row.asDict()}"
            )
        
    return True

def compute_area(row: Row) -> float: 
    """
     Takes a Row, validates it, determines what shape it is, and returns the area.
        Args:
            row (Row): A Spark Row with shape data.
        
        Returns:
            float: Area of the shape, or None if something goes wrong.
        
        Logs:
            Any errors during shape setup or area calculation.
    """
    try:
        shape_type = row.type.lower()

        shape = initialise_shape_class(shape_type, row)

        if not validate_row_fields(row, shape.required_fields):
            logger.error(f"Field validation failed for shape {shape_type}")
            return 0.0
        
        return shape.calculate_area()
    
    except Exception as e:
        logger.error(f"Error computing area for row {row}: {e}")
        return 0.0

## Using lower()
# All shape types in the dataset are already lowercase.
# I’ve kept the line: df = df.withColumn("type", lower(col("type"))) as a safeguard —
# assuming the shape "type" will either be spelled correctly or else caught as 
# an invalid shape in initialise_shape()),
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
    shape_data = spark.read.json('src/de_challenge/data_lines.jsonl') 
    shape_data = shape_data.withColumn("type", lower(col("type")))
    areas_rdd = shape_data.rdd.map(compute_area)

    total_area = areas_rdd.sum()
    logger.info(f"Total area: {total_area:.2f}")
    
def main():
    process_shapes()

if __name__ == "__main__":
    process_shapes()