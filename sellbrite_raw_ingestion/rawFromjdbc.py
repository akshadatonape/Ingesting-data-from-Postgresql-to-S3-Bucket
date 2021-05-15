
from python_app.utils.DynamoDBIntegration import dynamodbConnection
from python_app.utils import conn_sm
from datetime import datetime
from pyspark.sql import SparkSession
import logging
import boto3
import sys


def rawfromjdbc(table_names, list_of_columns, host_name):
    columns_string = ",".join(list_of_columns)
    logger.info("Creating Spark Query for %s ", table_names)
    query = f"select {columns_string} from {table_names} where updated_at>'{max_of_timestamp}'"
    print(query)
    #url_new = url.replace("host_name", host_name)

    logger.info("Creating %s Spark Dataframe ", table_names)

    try:
        Tables = (spark.read.format("jdbc")
                .option("url", url)
                .option("user", jdbcUsername)
                .option("password", jdbcPassword)
                .option("driver", drivers)
                .option("query", query)
                .load())

        logger.info("Showing %s Spark Dataframe ", table_names)

        Tables.show(10)
        logger.info("Writing %s Spark Dataframe to S3 ", table_names)
        s3 = boto3.client('s3')
        bucket_name = "aws-poc-serverless-analytics"
        today = datetime.now()
        directory_name = "emr/jdbc_ingetion/sellbrite_ingestion/"+today.strftime('%Y-%m-%d')#it's name of your folders
        s3.put_object(Bucket=bucket_name, Key=(directory_name+'/'))
        Tables.write.parquet(sys.argv[2]+today.strftime('%Y-%m-%d')+"/"+table_names,mode='overwrite')


    except Exception as e:
        logger.info("Not able to read %s table ", table_names)
        print(e)


def extract_tables_columns(host_name):
    logger.info("Listing down tables and columns of %s host", host_name)

    dict_of_tables_columns = dictionary_host_table_and_cols[host_name].asDict()
    tables = list(dict_of_tables_columns.keys())
    columns = list(dict_of_tables_columns.values())
    list(map(rawfromjdbc, tables, columns, [host_name]*len(tables)))

if __name__ == "__main__":

    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    logging.info("Creating Spark Session")

    spark = (SparkSession
             .builder
             .appName("Sellbrite Ingestion")
             .config("spark.jars.packages", "org.postgresql:postgresql:42.2.18")
             .getOrCreate())
#dataset_info_dev
    #now = datetime.now()
    #dt_end_string = now.strftime("%Y-%m-%d %H:%M:%S")

    logging.info("Getting Credentials from Secret Manager")

    url, jdbcUsername, jdbcPassword ,jdbcHostName = conn_sm.getJdbcUrl()
    drivers = "org.postgresql.Driver"
    data_from_dynamodb=dynamodbConnection()
    list_of_timestamp=list(map(lambda x:x['watermark_ts'],data_from_dynamodb))
    max_of_timestamp=max(list_of_timestamp)


try:
    logger.info("Mapping HostName with Tables and its Columns")

    jdbcSrcSchemaTables = spark.read.option("multiline", "true").json(sys.argv[3])

    host_table_and_cols = map(lambda row: row.asDict(), jdbcSrcSchemaTables.collect())
    dictionary_host_table_and_cols = list(host_table_and_cols)[0]
    extract_tables_columns(jdbcHostName)

except Exception as e:
    logging.info("Not able to read Json file")
    print(e)






    
