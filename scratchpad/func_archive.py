
# def initialise_shape(shape_data: dict)-> Shape:
#     shape_type = shape_data.get("type", "").lower()
#     shape_classes = { "triangle": Triangle,
#                      "circle": Circle,
#                      "rectangle": Rectangle}
    
#     if shape_type in shape_classes:
#         shape_class = shape_classes[shape_type]
#         shape = shape_class(shape_data)
#         return shape
        

# def check_shape_validity(shape_data: DataFrame) -> Shape:
#     shape_type = shape_data.withColumn("type", lower(col("type")))
#     valid_shapes = ["triangle", "circle", "rectangle"]
#     if shape_type in valid_shapes:
#             shape = initialise_shape_class(shape_type, shape_data)
#     else:
#         raise ValueError(f"Invalid shape type: {shape_type}. Must be one of {valid_shapes}")
#     return shape 


# def process_shapes():
#     df = df.withColumn("type", lower(col("type")))
#     areas = []
    
#     for row in df.collect():
#         shape_type = row.type
#         shape = initialise_shape_class(shape_type, row)
#         area = shape.calculate_area()
#         areas.append(area)
    
#     total_area = sum(areas)
#     print(total_area)


