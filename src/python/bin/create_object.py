from pyspark.sql import SparkSession
import logging
import logging.config


def get_spark_object(env, appname):
    logging.config.fileConfig(fname="../util/logging_to_file.conf")
    logger = logging.getLogger("create_object")

    try:
        logger.info(f"Environment veriable value is {env} ")
        if env == 'Test':
            master = 'local[3]'
        else:
            master = 'yarn'
        spark = SparkSession.builder \
            .master(master) \
            .appName(appname) \
            .config('spark.driver.bindAddress', 'localhost').config('spark.ui.port', '4050') \
            .getOrCreate()
    except NameError as ex:
        logger.error("NameError occured please check Trace file" + str(ex),exc_info=True)
        raise
    except Exception as exp:
        logger.error("Exception is occured and check Trace file" + str(exp),exc_info=True)
    else:
        logger.info("Spark Object created...")
    return spark;
