import logging
import logging.config

logging.config.fileConfig(fname="../util/logging_to_file.conf")
logger = logging.getLogger("validations")


def get_curr_date(spark):
    try:
        df = spark.sql("""select current_date""")
    except NameError as ex:
        logger.error("NameError occured please check trace for more details" + str(ex), exc_info=True)
    except Exception as exp:
        logger.error("Exception occured please check trace file" + str(exp), exc_info=True)
    else:
        logger.info("Validated spark object printing current date :" + str(df.collect()))


def df_count(df, dfname):
    try:
        logger.info(f"Data frame validation by the count by count() is started for dataframe {dfname}")
        count = df.count()
        logger.info(f"Count of dataframe is {count}")
    except Exception as exp:
        logger.info("Exception occured while counting values please check Tracefile" + str(exp), exc_info=True)
        raise
    else:
        logger.info("Count of datafram has been validated")


def df_top10_record(df, dfname):
    try:
        logger.info(f"Fetching top 10 records from dataframe {dfname}")
        top10_rec = df.limit(10).toPandas()
        logger.info("\n\t" + top10_rec.to_string(index=False))
    except Exception as exp:
        logger.error("Error occured while featching data please check Trace file" + str(exp), exc_info=True)
        raise
    else:
        logger.info("DataFram top 10 rows has been validate")


def df_print_schema(df, dfname):
    try:
        logger.info(f"df_print_schema() function execution on dataframe {dfname} has been started...")
        sch = df.schema.fields
        for i in sch:
            logger.info(f"\t{i}")
    except Exception as exp:
        logger.info("Exception occurred in function df_print_schem(). Please check trace file" + str(exp), exc_info=True)
    else:
        logger.info("df_print_schema() job has been completed")
