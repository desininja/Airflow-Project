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


jdbc_url = "jdbc:redshift://redshift-cluster-1.cqsv0dlaf0ry.us-east-1.redshift.amazonaws.com:5439/dev"
aws_iam_role = "arn:aws:iam::767398036887:role/service-role/AmazonRedshift-CommandsAccessRole-20240404T130502"
temp_dir="s3://food-delivery-project/temp-folder/"
target_table = 'food_delivery'
table_schema = "order_id int, customer_id int, restaurant_id int, order_time timestamp, customer_location string, restaurant_location string, order_value double, rating double, delivery_time timestamp"
create_table_query = f"""CREATE TABLE IF NOT EXISTS {target_table} ({table_schema})
                                 USING io.github.spark_redshift_community.spark.redshift"""

#writing data to redshift with options
try:
    df_transformed.write \
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url",jdbc_url) \
        .option("dbtable",target_table) \
        .option("tempdir",temp_dir) \
        .option("aws_iam_role", aws_iam_role) \
        .mode("overwrite") \
        .save()
    print(f"Data written to Redshift table: {target_table}")
except Exception as e:
    print(f"Error writing data  to Redshift: {e}")
    
        # Stop the Spark session
spark.stop()
