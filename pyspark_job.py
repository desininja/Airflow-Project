import sys 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType, StructField, StringType,  DoubleType, TimestampType,IntegerType


def main(s3_file_path):
    
    spark = SparkSession.builder.appName("Transforming Food Delivery Data").getOrCreate()

    schema = StructType([
        StructField('order_id',IntegerType(),True),
        StructField('customer_id',IntegerType(),True),
        StructField('restaurant_id',IntegerType(),True),
        StructField('order_time',TimestampType(),True),
        StructField('customer_location',StringType(),True),
        StructField('restaurant_location',StringType(),True),
        StructField('order_value',DoubleType(),True),
        StructField('rating',DoubleType(),True),
        StructField('delivery_time',TimestampType(),True)
        ])


    df =spark.read.csv(s3_file_path,schema=schema,header=True)

    df.printSchema()

    df_validated = df.filter(
    (df.order_time.isNotNull()) &
    (df.delivery_time.isNotNull())&
    (df.order_value>0)
    )
    
    df_transformed = df_validated.withColumn('delivery_duration',
                                         (df_validated.delivery_time - df_validated.order_time).cast('long') / 60)


    df_transformed.show(4)

    low_threshold = 500
    high_threshold = 1200

    df_transformed = df_transformed.withColumn('order_category',
                                          when(col('order_value')<low_threshold,"Low")
                                          .when((col('order_value')>=low_threshold)& (col('order_value')<=high_threshold),'Medium')
                                          .otherwise("High"))

    df_transformed.show(4)

    #writing the transformed data into a staging area in Amazon Redshift
    output_s3_path = 's3://food-delivery-project/output-files/'
    df_transformed.write.csv(output_s3_path)
    
    '''
    # Configure Redshift connection settings
    redshift_url = "jdbc:redshift://your-redshift-cluster:5439/your-database"
    redshift_user = "your-username"
    redshift_password = "your-password"
    temp_s3_path = "s3://food-delivery-project/temp-folder/"  # Temporary data path in S3 for Redshift


    df_transformed.write \
        .format("com.databricks.spark.redshift") \
        .option('url',redshift_url) \
        .option('user',redshift_user) \
        .option('password',redshift_password) \
        .option('dbtable','food_delivery_data') \
        .option('tempdir',temp_s3_path) \
        .mode("append")\
        .save()
    '''
    spark.stop()

if __name__ == "__main__":
    import sys 
    s3_file_path = sys.argv[1]
    print(f"Processing file: {s3_file_path}")
    main(s3_file_path)

