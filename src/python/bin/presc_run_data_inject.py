import logging
import logging.config


def load_filter(spark, file_format, inferSchema, file_dir, header):
    logging.config.fileConfig(fname="../util/logging_to_file.conf")
    logger = logging.getLogger(__name__)

    try:
        logger.info("Presc data injection job started...")
        if file_format == 'csv':
            df = spark \
                .read \
                .format(file_format) \
                .option(inferSchema=inferSchema) \
                .option(header=header) \
                .load(file_dir)
        else:
            df = spark \
                .read \
                .format(file_format) \
                .load(file_dir)
    except Exception as exp:
        logger.error("Exception occured please check the trace file " + str(exp), exe_info=True)
        raise
    else:
        logger.info(f"file {file_dir} is loaded into dataframe ")

    return df
