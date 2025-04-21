""" 
This module handles shape creation and area calculation.

Includes:
    Shape abstract class
    Triangle, Circle and Rectangle classes

"""


from abc import ABC, abstractmethod
from pyspark.sql import Row
from math import pi

from .logging import logger


## DICTIONARIES → ROWS
# Since this was my first time using PySpark, I initially wrote the shape classes to accept standard Python dictionaries.
# For example:
#   class Triangle(Shape):
#       def __init__(self, data: Dict):
#           self.base = float(data["base"])
#           self.height = float(data["height"])
# Once I had the basic behavior working, I added proper error handling using try/except blocks.
# After that, I updated the constructors to work with DataFrame Row objects instead of dictionaries.

# I've type-cast the Shape attributes (like base, height, and radius) and the return value of `calculate_area()` as float.
# While the dataset provides integer values, calculating the area of a circle involves pi, which returns a float.
# After exploring PySpark, I now realise I could have defined a schema to enforce types —
# which would likely be a cleaner and more robust approach than the manual typecasting I'm using here.

class Shape(ABC):
    "An abstract base class for geometric shapes that can calculate their area"

    def __init__(self, row: Row):
        logger.info(f"Initializing base Shape with data: {row}")

    @abstractmethod
    def calculate_area(self)-> float:
        """Subclasses must implement this method to return the shapes area"""
        pass
    

class Triangle(Shape):
    def __init__(self, row: Row):
        """
        Initializes a Triangle instance.
        Args:
            row (Row): A Row containing 'base' and 'height' fields.
        Raises:
            ValueError: If base or height is missing or less than/equal to zero.
            TypeError: If base or height cannot be cast to float
            AttributeError: If base or height are not present. 
        """
        try:
            self.base = float(row.base)
            self.height = float(row.height)

            if self.base <= 0 or self.height <= 0:
                error_message = f"Base and height must be greater than zero. Received base={self.base}, height={self.height}"
                logger.error(error_message)
                raise ValueError(error_message)
           
        except (KeyError, TypeError, AttributeError) as e:
            logger.error(f"Error initializing Triangle: {e}")
            raise

        logger.info(f"Initialized Triangle with base={self.base}, height={self.height}")
    
 
    def calculate_area(self) -> float:
        """Returns the area of the triangle as a float."""
        area = (self.base * self.height) / 2
        logger.info(f"Triangle area={area} with base={self.base}, height={self.height}")
        return area
        

class Circle(Shape):
    def __init__(self, row: Row):
        """
        Initializes a Circle instance.
        Args:
            row (Row): A Row containing 'radius' field.
        Raises:
            ValueError: If radius is missing or less than/equal to zero.
            TypeError: If radius cannot be cast to float
            AttributeError: If radius is not present. 
        """
        try:
            self.radius = float(row.radius)

            if self.radius <= 0:
                error_message = f"Radius must be greater than zero. Received radius={self.radius}"
                logger.error(error_message)
                raise ValueError(error_message)
           
        except (KeyError, TypeError, AttributeError) as e:
            logger.error(f"Error initializing Circle: {e}")
            raise

        logger.info(f"Initialized Circle with radius={self.radius}")
  


    def calculate_area(self)-> float:
        """Returns the area of the circle as a float."""
        area = pi * self.radius ** 2
        logger.info(f"Circle area={area} with radius={self.radius}")
        return area


class Rectangle(Shape):
   def __init__(self, row: Row):
        """
        Initializes a Triangle instance.
        Args:
            row (Row): A Row containing 'width' and 'height' fields.
        Raises:
            ValueError: If width or height is missing or less than/equal to zero.
            TypeError: If width or height cannot be cast to float
            AttributeError: If width or height are not present. 
        """
        try:
            self.width = float(row.width)
            self.height = float(row.height)

            if self.width <= 0 or self.height <= 0:
                error_message = f"Width and height must be greater than zero. Received width={self.width}, height={self.height}"
                logger.error(error_message)
                raise ValueError(error_message)
           
        except (KeyError, TypeError, AttributeError) as e:
            logger.error(f"Error initializing Rectangle: {e}")
            raise

        logger.info(f"Initialized Rectangle with width={self.width}, height={self.height}")

   def calculate_area(self)-> float:
        """Returns the area of the rectangle as a float."""
        area = self.width * self.height
        logger.info(f"Rectangle area={area} with width={self.width}, height={self.height}")
        return area