from pyspark.sql.functions import count, udf
from pyspark.sql.types import IntegerType


@udf(returnType=IntegerType())
def column_split_cnt(zip):
    return len(zip.split(" "))
