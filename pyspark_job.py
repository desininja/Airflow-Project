
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType


s3_file_path = 's3://food-delivery-project/landing-zone/' #sys.argv[1]
# Create a Spark session
spark = SparkSession.builder.appName("Transforming Food Delivery Data").getOrCreate()

# Define schema
schema = StructType([
            StructField('order_id', IntegerType(), True),
            StructField('customer_id', IntegerType(), True),
            StructField('restaurant_id', IntegerType(), True),
            StructField('order_time', TimestampType(), True),
            StructField('customer_location', StringType(), True),
            StructField('restaurant_location', StringType(), True),
            StructField('order_value', DoubleType(), True),
            StructField('rating', DoubleType(), True),
            StructField('delivery_time', TimestampType(), True)
        ])

        # Read the data from S3
df = spark.read.csv(s3_file_path, schema=schema, header=True)
df.printSchema()

        # Validate the data
df_validated = df.filter(
            (df.order_time.isNotNull()) &
            (df.delivery_time.isNotNull()) &
            (df.order_value > 0)
        )

        # Transform the data
df_transformed = df_validated.withColumn('delivery_duration',
                                                 (df_validated.delivery_time - df_validated.order_time).cast('long') / 60)
df_transformed.show(4)

        # Categorize orders based on value
low_threshold = 500
high_threshold = 1200

df_transformed = df_transformed.withColumn('order_category',
                                                   when(col('order_value') < low_threshold, "Low")
                                                   .when((col('order_value') >= low_threshold) & (col('order_value') <= high_threshold), 'Medium')
                                                   .otherwise("High"))

df_transformed.show(4)

        # Writing the transformed data into a staging area in Amazon S3
output_s3_path = 's3://food-delivery-project/output-files/first_file.csv'
df_transformed.write.csv(output_s3_path)

        # Stop the Spark session
spark.stop()
