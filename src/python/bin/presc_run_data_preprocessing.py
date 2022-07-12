from pyspark.sql.functions import upper, lit, expr,col, regexp_extract,count,isnan,isnull, when, concat_ws, coalesce, avg, round
import logging
import logging.config
from pyspark.sql.window import Window


from pyspark.sql import functions as f
import pandas

logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__)


def perform_data_cleanup(df1, df2):
    try:
        logger.info("perform_data_cleanup() is started on df_city...")
        df_city_sel = df1.select(upper("city").alias("city"), "state_id", upper("state_name").alias("state_name"),
                                 upper("county_name").alias("county_name"), "population", "zips")
        logger.info("perform_data_cleanup() is started on df_fact...")
        df_fact_sel = df2.select(expr("npi as presc_id"),expr("nppes_provider_last_org_name as presc_lname"),\
                         expr("nppes_provider_first_name as presc_fname"), expr("nppes_provider_city as presc_city"),\
                         expr("nppes_provider_state as pres_state"),expr("specialty_description as presc_spclt"),\
                         "years_of_exp", "drug_name",expr("total_claim_count as trx_cnt"), "total_day_supply","total_drug_cost")

        df_fact_sel=df_fact_sel.withColumn("city",lit("USA"))
        idx=0
        pattern='\d+'
        df_fact_sel=df_fact_sel.withColumn("years_of_exp",regexp_extract(col("years_of_exp"),pattern,idx))
        df_fact_sel=df_fact_sel.withColumn("years_of_exp",col("years_of_exp").cast("int"))

        #Concate name and fname
        df_fact_sel=df_fact_sel.withColumn("presc_fullname",concat_ws(" ","presc_fname","presc_lname"))
        #Drop columns, presc_fname and presc_lname
        df_fact_sel=df_fact_sel.drop("presc_fname","presc_lname")

        #df_fact_sel.select([count(when (isnan(c) | isnull(c),c)).alias(c) for c in df_fact_sel.columns]).show()
        # Drop drug_name na entries
        df_fact_sel=df_fact_sel.na.drop(subset="presc_id")
        # Drop drug_name na entries
        df_fact_sel = df_fact_sel.na.drop(subset="drug_name")

        #Calculate trx_cnt average and fill na values with the same.
        win=Window.partitionBy("presc_id")
        df_fact_sel=df_fact_sel.withColumn("trx_cnt",coalesce("trx_cnt",round(avg("trx_cnt").over(win))))

        df_fact_sel.select([count(when(isnan(c) | isnull(c), c)).alias(c) for c in df_fact_sel.columns]).show()
        #df_fact_sel.where(expr("trx_cnt is null")).show()
    except Exception as exp:
        logger.error("Error in method perform_data_cleanup(). Please check stack Trace:" + str(exp), exc_info=True)
        raise
    else:
        logger.info("perform_data_cleanup() is completed on df_city and df_fact.")
    return df_city_sel, df_fact_sel
