from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import datetime

from hl7_parser import HL7_Parser

'''
Read a HL7 files into a Dataframe

to execute:
cd /home/assamese/work/python-projects/spark-python-hl7
spark-submit spark_hl7.py
'''
from pyspark.sql.functions import udf


def clean_datetime(dt):
    return dt[:8]


class SparkApp:

    @staticmethod
    def run(sparkContext, folder_name):

        SparkApp.logger.info(sparkContext.appName + "Starting run()")

        hl7_pair_rdd = sparkContext.wholeTextFiles(folder_name + "*Covid*.txt")
        #print(hl7_pair_rdd.take(1))
        hl7_rdd = hl7_pair_rdd.map(lambda x: x[1])
        #print(hl7_rdd.take(10))

        # DO NOT COMMENT THIS LINE
        dummySparkSession = SparkSession(sparkContext) # see this: https://stackoverflow.com/questions/32788387/pipelinedrdd-object-has-no-attribute-todf-in-pyspark

        df_hl7 = hl7_rdd.map(lambda x: (HL7_Parser.get_patient_id_from_RDD(x)
                                        , HL7_Parser.get_message_type_from_RDD(x)
                                        , HL7_Parser.get_observation_datetime_from_RDD(x)
                                        , x
                                        , datetime.datetime.today().strftime('%Y-%m-%d-%H:%M:%S')
                                        ))\
            .toDF(["patient_id", "message_type", "observation_datetime", "message_content", "system_timestamp"])

        print(df_hl7.show())

        clean_datetime_udf = udf(clean_datetime)

        df_cleaned_datetime = df_hl7.withColumn("observation_datetime_cleaned", clean_datetime_udf("observation_datetime"))

        print(df_cleaned_datetime.show())

        SparkApp.logger.info(sparkContext.appName + "Ending run()")


if __name__ == "__main__":
    app_name = "hl7-Pipeline-Step1~"
    master_config = "local[3]"  # bin/spark-shell  --master local[N] means to run locally with N threads
    conf = SparkConf().setAppName(app_name).setMaster(master_config)
    sparkContext = SparkContext(conf=conf)
    sparkContext.setLogLevel("INFO")

    log4jLogger = sparkContext._jvm.org.apache.log4j
    SparkApp.logger = log4jLogger.LogManager.getLogger(__name__)

    # sqlContext = SQLContext(sparkContext)
    print("------------------------------ " + app_name + " Spark-App-start -----------------------------------------")
    folder_name = '/home/assamese/work/python-projects/spark-python-hl7/hl7-data/'
    SparkApp.run(sparkContext, folder_name)

    print("------------------------------- " + app_name + " Spark-App-end ------------------------------------------")
