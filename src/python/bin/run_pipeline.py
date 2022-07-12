import sys
import os
import get_all_veriables as gav
from create_object import get_spark_object
import facts_run_data_inject
from validations import get_curr_date, df_count, df_top10_record, df_print_schema
import logging
import logging.config
import presc_run_data_inject
import datetime
from presc_run_data_preprocessing import perform_data_cleanup
from presc_run_data_transform import city_report, prescribe_report

logging.config.fileConfig(fname="../util/logging_to_file.conf")


def main():
    try:

        # Create Spark object
        spark = get_spark_object(gav.environment, gav.app_name)

        # Validate Spark Object
        get_curr_date(spark)
        logging.info("Spark object validated")

    except Exception as ex:
        logging.error("Exception occured in main() methon. Please check Tracefile for more details." + str(ex),
                      exc_info=True)
        sys.exit(1)
    spark.conf.set("spark.sql.shuffle.partitions",50)
    for file in os.listdir(gav.staging_dim_city):
        logging.info("file path is " + file)
        file_dir = gav.staging_dim_city + '\\' + file
        logging.info("File name with path " + file_dir)

        if file.split(".")[1] == 'csv':
            file_format = 'csv'
            header = gav.header
            inferSchema = gav.inferSchema

        elif file.split(".")[1] == 'parquet':
            file_format = 'parquet'
            header = "NA"
            inferSchema = "NA"

    df_city = presc_run_data_inject.load_filter(spark, file_format, inferSchema, file_dir, header)
    '''res=df_city.limit(10).toPandas()
    logging.info("\n\t"+res.to_String(index=False))'''

    df_count(df_city, "df_city")
    df_top10_record(df_city, "df_city")

    # Load facts data file into datafram
    for file in os.listdir(gav.facts):
        logging.info("file path is " + file)
        file_dir = gav.facts + '\\' + file
        logging.info("File name with path " + file_dir)

        if file.split(".")[1] == 'csv':
            file_format = 'csv'
            header = gav.header
            inferSchema = gav.inferSchema

        elif file.split(".")[1] == 'parquet':
            file_format = 'parquet'
            header = "NA"
            inferSchema = "NA"

    df_fact = facts_run_data_inject.load_filter(spark, file_format, inferSchema, file_dir, header)
    df_count(df_fact, "df_fact")
    df_top10_record(df_fact, "df_fact")

    # DataFrame Preprocessing
    df_city_clean, df_fact_clean = perform_data_cleanup(df_city, df_fact)
    df_top10_record(df_city_clean, "df_city_clean")
    df_top10_record(df_fact_clean, "df_fact_clean")

    df_print_schema(df_city_clean, "DF_City_Clean")
    df_print_schema(df_fact_clean, "Df_Fact_Clean")


    '''city_final_report=city_report(df_city_clean, df_fact_clean)
    df_top10_record(city_final_report,"City_final_report")
    df_print_schema(city_final_report,"city_final_report")
    '''
    '''presc_final_report=prescribe_report(df_city)
    df_top10_record(presc_final_report,"Presc_final_report")
    df_print_schema(presc_final_report,"presc_final_report")'''


if __name__ == "__main__":
    main()
    input("enter any to exit")
    '''spark=SparkSession.builder \
    .master("local[3]").getOrCreate()
    print("Spark object created...")
    print(spark)'''
