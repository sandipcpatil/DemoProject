from udfs import column_split_cnt
from pyspark.sql.functions import countDistinct, sum, dense_rank, col, desc
from pyspark.sql.window import Window
import logging
import logging.config

logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__)


def city_report(df_city_sel, df_fact_sel):
    try:
        logger.info("Data transformation job has been started...")
        df_city_split = df_city_sel.withColumn("zip_count", column_split_cnt(df_city_sel.zips))
        # df_city_split.select("city", "state_name", "population", "county_name", "zips", "zip_count").show()
        df_fact_grp = df_fact_sel.groupBy("pres_state", "presc_city").agg(
            countDistinct("presc_id").alias("presc_cnt"), sum("trx_cnt").alias("trx_counts"))

        # df_city_join = df_city_split.join(df_fact_grp, (df_city_split.state_name == df_fact_grp.presc_state) & (
        #           df_city_split.city == df_fact_grp.presc_city), 'inner')
        # df_city_final = df_city_join.select("city", "state_name", "county_name", "population", "zip_counts",
        #                                  "trx_counts", "presc_counts")

        df_city_final = df_city_split.join(df_fact_grp, (df_fact_grp.pres_state == df_city_split.state_name) & (
                    df_city_split.city == df_fact_grp.presc_city), 'inner')
        # df_city_final.show(10)
    except Exception as exp:
        logger.error("Exception occured in city_report(), please check trace message"+str(exp), exc_info=True)
        raise
    else:
        logger.info("City report has been created sucessfully...")

    return df_city_final


def prescribe_report(df_fact_sel):
    try:
        logger.info("Prescirbe report() has been started...")
        partrank= Window.partitionBy("pres_state").orderBy(col("trx_cnt").desc())
        '''df_presc_final=df_fact_sel.select("presc_fullname","pres_state","presc_city","years_of_exp"
                                          "trx_cnt","total_drug_cost").filter("years_of_exp => 20 and years_of_exp<=50")\
                                        .withColumn("dense_rank",dense_rank().over(partrank))\
                                        .filter("dense_rank <= 5")'''
        print(df_fact_sel.printSchema())
        ts=df_fact_sel.schema.fields
        for i in ts:
            print(f"\t{i}")

    except Exception as exp:
        logger.error("EXception has been occured in prescribe_report(). Please check trace error" + str(exp),exc_info=True)
        raise
    else:
        logger.info("Prescirbe report() has been completed sucessfully...")

    return partrank