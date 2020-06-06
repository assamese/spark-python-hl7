from pyspark import SparkContext, SparkConf, SQLContext
from hl7_parser import HL7_Parser
from config_framework import ConfigFramework

'''
Read df from Postgres
clean, transform


to execute:
cd /home/assamese/work/python-projects/spark-python-hl7
spark-submit --conf spark.driver.extraJavaOptions='-Dcom.amazonaws.services.s3.enableV4' --conf spark.executor.extraJavaOptions='-Dcom.amazonaws.services.s3.enableV4'   --packages org.apache.hadoop:hadoop-aws:2.7.1 --driver-class-path /home/assamese/work/postgres-jdbc/postgresql-42.2.12.jar spark_hl7_pipeline_step2.py
'''
from pyspark.sql.functions import udf


def clean_datetime(dt):
    return dt[:8]

def get_observation_datetime(hl7_message):
    return HL7_Parser.get_observation_datetime_from_RDD(hl7_message) # '201903011257-0500'

class SparkApp:

    @staticmethod
    def run(sparkContext, src_table_name):
        SparkApp.logger.info(sparkContext.appName + "Starting run()")

        sqlContext = SQLContext(sparkContext)
        SparkApp.logger.info(sparkContext.appName + "Starting jdbc read() !")
        df_hl7 = sqlContext.read.jdbc(url=ConfigFramework.getPostgres_URL()
                                        , table=src_table_name
                                        , properties=ConfigFramework.getPostgres_Properties())
        SparkApp.logger.info(sparkContext.appName + "End jdbc read() !")
        print(df_hl7.show())

        get_observation_datetime_udf = udf(get_observation_datetime)
        df_with_observation_datetime = df_hl7.withColumn("observation_datetime"
                                                         , get_observation_datetime_udf("message_content"))
        # print(df_with_observation_datetime.show())

        clean_datetime_udf = udf(clean_datetime)

        df_cleaned_datetime = df_with_observation_datetime.withColumn("observation_datetime_cleaned"
                                                , clean_datetime_udf("observation_datetime"))

        print(df_cleaned_datetime.show())

        SparkApp.logger.info(sparkContext.appName + "Ending run()")


if __name__ == "__main__":
    app_name = "hl7_pipeline_step2~"
    master_config = "local[3]"  # bin/spark-shell  --master local[N] means to run locally with N threads
    conf = SparkConf().setAppName(app_name).setMaster(master_config)
    sparkContext = SparkContext(conf=conf)
    sparkContext.setLogLevel("INFO")

    log4jLogger = sparkContext._jvm.org.apache.log4j
    SparkApp.logger = log4jLogger.LogManager.getLogger(__name__)

    # sqlContext = SQLContext(sparkContext)
    print("------------------------------ " + app_name + " Spark-App-start -----------------------------------------")
    table_name = 'hl7_Pipeline_Step1_sink'
    SparkApp.run(sparkContext, table_name)

    print("------------------------------- " + app_name + " Spark-App-end ------------------------------------------")
