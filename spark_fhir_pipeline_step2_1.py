from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.functions import *

'''
Read a FHIR files into a Dataframe
extract key-columns into result_df
write result-df to a Postgres Table

to execute:
cd /home/assamese/work/python-projects/spark-python-hl7
spark-submit --conf spark.driver.extraJavaOptions='-Dcom.amazonaws.services.s3.enableV4' --conf spark.executor.extraJavaOptions='-Dcom.amazonaws.services.s3.enableV4'   --packages org.apache.hadoop:hadoop-aws:2.7.1 --driver-class-path /home/assamese/work/postgres-jdbc/postgresql-42.2.12.jar spark_fhir_pipeline_step2_1.py
'''


def clean_datetime(dt):
    # convert 2016-06-24T10:38... to 20160624
    x = dt[:10]
    return x.replace("-", "")


def extract_patient_id(subject_reference):
    # extract 1PID13295039 from Patient/1PID13295039
    l = subject_reference.split('/')
    return l[1]


class SparkApp:

    @staticmethod
    def run(sqlContext, folder_name, file_name, table_name):

        SparkApp.logger.info(sparkContext.appName + "Starting run()")

        df_fhir = sqlContext\
            .read\
            .option("multiLine", True)\
            .option("mode", "PERMISSIVE")\
            .json(folder_name)
        print(df_fhir.show())

        df_fhir_entry_list = df_fhir.select(
            df_fhir.id
            ,df_fhir.entry.alias("entry_list"))

        print(df_fhir_entry_list.printSchema())

        df_fhir_entry_list_0 = (df_fhir_entry_list.select(col("id"), col("entry_list").getItem(0).alias("entry_list_0")))

        #df_fhir_entry_list_0.show()
        #df_fhir_entry_list_0.printSchema()

        df_fhir_entry_list_0_exploded = (
            df_fhir_entry_list_0.select(col("id")
                                        , col("entry_list_0").resource.medicationReference.display.alias("resource_medicationReference_display")
                                        , col("entry_list_0").resource.subject.reference.alias("resource_subject_reference")
                                        , col("entry_list_0").resource.requester.reference.alias("resource_requester_reference")
                                        , col("entry_list_0").resource.authoredOn.alias("resource_authoredOn")
                                        )
        )
        #df_fhir_entry_list_0_exploded.show()

        clean_datetime_udf = udf(clean_datetime)
        extract_patient_id_udf = udf(extract_patient_id)

        df_cleaned_datetime = df_fhir_entry_list_0_exploded\
            .withColumn("resource_authoredOn_dt", clean_datetime_udf("resource_authoredOn"))\
            .withColumn("patient_id", extract_patient_id_udf("resource_subject_reference"))
        df_cleaned_datetime.show()

        SparkApp.logger.info(sparkContext.appName + "Ending run()")


if __name__ == "__main__":
    app_name = "fhir_pipeline_step2_1~"
    master_config = "local[3]"  # bin/spark-shell  --master local[N] means to run locally with N threads
    conf = SparkConf().setAppName(app_name).setMaster(master_config)
    sparkContext = SparkContext(conf=conf)
    sparkContext.setLogLevel("INFO")

    log4jLogger = sparkContext._jvm.org.apache.log4j
    SparkApp.logger = log4jLogger.LogManager.getLogger(__name__)

    sqlContext = SQLContext(sparkContext)
    print("------------------------------ " + app_name + " Spark-App-start -----------------------------------------")
    folder_name = '/home/assamese/work/python-projects/spark-python-hl7/fhir-data/'
    file_name = 'sample_medicationrequest_1.json'
    table_name = 'fhir_Pipeline_Step2_1_sink'
    SparkApp.run(sqlContext, folder_name, file_name, table_name)

    print("------------------------------- " + app_name + " Spark-App-end ------------------------------------------")
