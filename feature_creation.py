from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, lag, lead, when
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder.appName("Stock Features").getOrCreate()

# Database connection properties
database_url = "jdbc:postgresql://localhost:5432/your_database"
properties = {"user": "your_username", "password": "your_password", "driver": "org.postgresql.Driver"}
