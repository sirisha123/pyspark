from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = None
try:
        from databricks.connect import DatabricksSession
        spark = DatabricksSession.builder.getOrCreate()
except ImportError:
        spark = SparkSession.builder.getOrCreate()

# Sample data
data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]

# Define schema
schema = StructType([
    StructField("Name", StringType(), True),
    StructField("ID", IntegerType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show DataFrame
df.show()