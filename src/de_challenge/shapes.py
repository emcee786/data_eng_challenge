""" 
This module handles shape creation and area calculation.

Includes:
    Shape abstract base class
    Triangle, Circle and Rectangle subclasses

    Shape ABC requires all subclasses to contain calculate_area() method
    Shapes are instatiated from pyspark "row" type using named columns
"""


from abc import ABC, abstractmethod
from math import pi

from pyspark.sql import Row

from .logging_utils import logger


class Shape(ABC):
    "An abstract base class for geometric shapes that can calculate their area"

    required_fields = []

    def __init__(self, row: Row):
        self.row = row
        logger.info(f"Initializing base Shape with data: {row}")

    @abstractmethod
    def calculate_area(self)-> float:
        """Subclasses must implement this method to return the shapes area"""
        pass
    

class Triangle(Shape):
    required_fields = ["base", "height"]
    
    def __init__(self, row: Row):
        """
        Initializes a Triangle instance.
        Args:
            row (Row): A Row containing 'base' and 'height' fields.

        """
        super().__init__(row) 
        self.base = float(row.base)
        self.height = float(row.height)
        logger.info(f"Initialized Triangle with base={self.base}, height={self.height}")
    
 
    def calculate_area(self) -> float:
        """Returns the area of the triangle as a float."""
        area = (self.base * self.height) / 2
        logger.info(f"Triangle area={area} with base={self.base}, height={self.height}")
        return area
        

class Circle(Shape):
    required_fields = ["radius"]

    def __init__(self, row: Row):
        """
        Initializes a Circle instance.
        Args:
            row (Row): A Row containing 'radius' field.
        """
        super().__init__(row)
        self.radius = float(row.radius)
        logger.info(f"Initialized Circle with radius={self.radius}")
  


    def calculate_area(self)-> float:
        """Returns the area of the circle as a float."""
        area = pi * self.radius ** 2
        logger.info(f"Circle area={area} with radius={self.radius}")
        return area


class Rectangle(Shape):
   required_fields = ["width", "height"]

   def __init__(self, row: Row):
        """
        Initializes a Triangle instance.
        Args:
            row (Row): A Row containing 'width' and 'height' fields.
        """
        super().__init__(row) 
        self.width = float(row.width)
        self.height = float(row.height)
        
   def calculate_area(self)-> float:
        """Returns the area of the rectangle as a float."""
        area = self.width * self.height
        logger.info(f"Rectangle area={area} with width={self.width}, height={self.height}")
        return area