import logging
import os
import sys

import boto3
from botocore.exceptions import ClientError
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

logging.basicConfig()
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(getattr(logging, os.environ.get('LOG_LEVEL', 'DEBUG')))
LOGGER.debug(__name__)
GLUE = boto3.client('glue')


def get_database(database):

    try:
        return GLUE.get_database(Name=database)['Database']
    except ClientError as err:
        LOGGER.warning("Database not found: %s", err)
        GLUE.create_database(
            DatabaseInput={
                'Name': database,
                'Description': 'gsg bike index',
                'Parameters': {
                    'System': 'bike'
                }
            })
    return GLUE.get_database(Name=database)['Database']

def create_table(database, table, columns, location, partition) -> dict:
    table_input = {
        'Name': table,
        'Description': 'bike lake',
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': dict(
            System='bike',
            External=str(True)),
        'StorageDescriptor': {
            'Columns': columns,
            'Location': location.replace("s3a", "s3"),
            'InputFormat': 'org.apache.hudi.hadoop.HoodieParquetInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
            'NumberOfBuckets': -1,
            'SerdeInfo': {'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                          "Parameters": {
                              "serialization.format": "1"
                          }}}}
    if partition:
        table_input['PartitionKeys'] = [dict(Name='part', Type='string')]
    return GLUE.create_table(
        DatabaseName=get_database(database)['Name'],
        TableInput=table_input)


def get_catalog(event, dataframe) -> dict:
    try:
        return boto3.client('glue').get_table(DatabaseName=event['database'], Name=event['table']).get('Table')
    except ClientError as e:
        if e.response['Error'].get('Code') == 'EntityNotFoundException':
            create_table(event['database'], event['table'], columns(dataframe, event), event['location'],
                         event['partition'])
    return boto3.client('glue').get_table(DatabaseName=event['database'], Name=event['table']).get('Table')


def columns(dataframe, event) -> list:
    columns = list()
    for col in dataframe.dtypes:
        name = str(col[0]).lower()
        type = str(col[1]).lower()
        if name != "part":
            columns.append(dict(
                Name=name,
                Type=type))

    return columns


def get_config_hudi(key, partition, table, tiebreaker):
    configs = {
        "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.ComplexKeyGenerator",
        "hoodie.datasource.write.payload.class": "org.apache.hudi.common.model.DefaultHoodieRecordPayload",
        "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
        "hoodie.table.name": table,
        "hoodie.datasource.write.recordkey.field": key,
        "hoodie.datasource.write.partitionpath.field": 'part',
        "hoodie.datasource.write.hive_style_partitioning": "true",
        "hoodie.datasource.write.precombine.field": tiebreaker,
        "hoodie.datasource.write.operation": "upsert",

        #"hoodie.write.concurrency.mode": "single_writer",
        "hoodie.cleaner.commits.retained": 2,

        "hoodie.bloom.index.use.caching": True,
        "hoodie.parquet.compression.codec": "snappy",
        #"hoodie.parquet.max.file.size": 64
    }
    return configs


def read_data_frame(spark, event):
    dataframe = spark.read.format(event['file_format']).load(event['raw_data'])
    return dataframe


def hudi_upsert(dataframe, config, event):
    try:
        dataframe.write.format("hudi").options(**config).mode("append").save(event['location'])
    except Exception as err:
        LOGGER.error('ERROR: %s', err)
        raise err

def apply_partition(dataframe, event):
    #todo: should be a function for any partition
    dataframe = dataframe.withColumn('part', from_unixtime(col(event['partition']), "yyyyMMdd"))
    return dataframe

def hudi_handler(event, spark):
    dataframe = read_data_frame(spark, event)
    dataframe = apply_partition(dataframe, event)

    catalog = get_catalog(event, dataframe)

    configs = get_config_hudi(event['key'], event['partition'], event['table'], event['tiebreaker'])

    #partition = event['partition'].split(',')

    hudi_upsert(dataframe, configs, event)

    #partitions_list = [row.asDict() for row in dataframe.select(partition).distinct().collect()]
    #todo: update partition catalog
    # update_catalog(events, paths, partitions_list)


def init_spark():
    conf = SparkConf()
    conf.set("spark.jars.packages",
             "org.apache.hudi:hudi-spark3-bundle_2.12:0.8.0," "org.apache.hadoop:hadoop-aws:3.2.0,")
    conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    conf.set("spark.sql.files.ignoreMissingFiles", "true")
    conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("spark.hadoop.fs.s3n.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("spark.hadoop.fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")
    conf.set("spark.hadoop.fs.AbstractFileSystem.s3n.impl", "org.apache.hadoop.fs.s3a.S3A")
    conf.set("spark.hadoop.fs.AbstractFileSystem.s3.impl", "org.apache.hadoop.fs.s3a.S3A")
    conf.set("spark.hadoop.fs.s3a.aws.credentials.provider",
                        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
                       #"com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
    spark = SparkSession.builder.master("local[*]").config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    return spark


def mock_event_bike_search():
    event = dict()
    event['key'] = 'hash'
    event['partition'] = 'bike_created_at'
    event['table'] = 'search'
    event['tiebreaker'] = 'current_ts'
    event['database'] = 'bikeindex'
    event['location'] = get_step() + 'analytics/bikeindex/search/'
    event['raw_data'] = 'raw_data/bike-search/'
    event['file_format'] = 'json'
    return event


def mock_event_bike():
    event = dict()
    event['key'] = 'id'
    event['partition'] = 'registration_created_at'
    event['table'] = 'bike'
    event['tiebreaker'] = 'registration_updated_at'
    event['database'] = 'bikeindex'
    event['location'] = get_step() + 'analytics/bikeindex/bike'
    event['raw_data'] = 'raw_data/bike/'
    event['file_format'] = 'json'
    return event


def mock_event_bike_stolen():
    event = dict()
    event['key'] = 'key'
    event['partition'] = 'bike_created_at'
    event['table'] = 'stolen'
    event['tiebreaker'] = 'bike_updated_at'
    event['database'] = 'bikeindex'
    event['location'] = get_step() + 'analytics/bikeindex/stolen/'
    event['raw_data'] = 'raw_data/bike-stolen/'
    event['file_format'] = 'json'
    return event

def get_step():
    for count, item in enumerate(sys.argv):
        if item == '--bucket_name':
            return (sys.argv[count + 1])
    return ""


if __name__ == "__main__":

    spark = init_spark()

    event_bike_search = mock_event_bike_search()
    hudi_handler(event_bike_search, spark)

    event_bike = mock_event_bike()
    hudi_handler(event_bike, spark)

    event_bike_stolen = mock_event_bike_stolen()
    hudi_handler(event_bike_stolen, spark)

