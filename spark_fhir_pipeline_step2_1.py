from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.functions import flatten, explode

from hl7_parser import HL7_Parser
from config_framework import ConfigFramework

'''
Read a HL7 files into a Dataframe
extract key-columns into result_df
write result-df to a Postgres Table

to execute:
cd /home/assamese/work/python-projects/spark-python-hl7
spark-submit --conf spark.driver.extraJavaOptions='-Dcom.amazonaws.services.s3.enableV4' --conf spark.executor.extraJavaOptions='-Dcom.amazonaws.services.s3.enableV4'   --packages org.apache.hadoop:hadoop-aws:2.7.1 --driver-class-path /home/assamese/work/postgres-jdbc/postgresql-42.2.12.jar spark_fhir_pipeline_step2_1.py
'''


class SparkApp:

    @staticmethod
    def run(sqlContext, folder_name, file_name, table_name):

        SparkApp.logger.info(sparkContext.appName + "Starting run()")

        df_fhir = sqlContext\
            .read\
            .option("multiLine", True)\
            .option("mode", "PERMISSIVE")\
            .json(folder_name + file_name)
        print(df_fhir.show())

        df_fhir_entry_list = df_fhir.select(
            df_fhir.id
            ,df_fhir.entry.alias("entry_list"))

        #print(df_fhir_entry_list.printSchema())


        df_fhir_entry_list_exploded = df_fhir_entry_list.select(
            df_fhir_entry_list.id,
            df_fhir_entry_list.entry_list.resource.resourceType.alias("resource_resource_Type"),
            df_fhir_entry_list.entry_list.resource.contained.alias("resource_contained"),
            df_fhir_entry_list.entry_list.resource.subject.reference.alias("resource_subject_reference"),
            df_fhir_entry_list.entry_list.resource.requester.reference.alias("resource_requester_reference"))


        #df_fhir_entry_list_exploded = df_fhir_entry_list.select(explode(df_fhir_entry_list.entry_list))
        print(df_fhir_entry_list_exploded.show())

        df_resource_subject_reference_exploded = df_fhir_entry_list_exploded.select(
                    df_fhir_entry_list_exploded.id
                    , df_fhir_entry_list_exploded.resource_resource_Type
                    , df_fhir_entry_list_exploded.resource_contained
                    , df_fhir_entry_list_exploded.resource_requester_reference
                    , explode(df_fhir_entry_list_exploded.resource_subject_reference).alias("resource_subject_reference")
                   )
        print(df_resource_subject_reference_exploded.show())

        df_resource_requester_reference_exploded = df_resource_subject_reference_exploded.select(
                    df_resource_subject_reference_exploded.id
                    , df_resource_subject_reference_exploded.resource_resource_Type
                    , df_resource_subject_reference_exploded.resource_contained
                    , df_resource_subject_reference_exploded.resource_subject_reference
                    , explode(df_resource_subject_reference_exploded.resource_requester_reference).alias("resource_requester_reference")
                    )

        print(df_resource_requester_reference_exploded.show())

        df_resource_resource_Type_exploded = df_resource_requester_reference_exploded.select(
                    df_resource_requester_reference_exploded.id
                    , df_resource_requester_reference_exploded.resource_contained
                    , df_resource_requester_reference_exploded.resource_requester_reference
                    , df_resource_requester_reference_exploded.resource_subject_reference
                    , explode(df_resource_requester_reference_exploded.resource_resource_Type).alias("resource_resource_Type")
                    )
        print(df_resource_resource_Type_exploded.show(df_resource_resource_Type_exploded.count(), False))

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
    table_name = 'hl7_Pipeline_Step2_1_sink'
    SparkApp.run(sqlContext, folder_name, file_name, table_name)

    print("------------------------------- " + app_name + " Spark-App-end ------------------------------------------")
