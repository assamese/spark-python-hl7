from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.functions import *
from config_framework import ConfigFramework

'''
Read 2 dfs from Postgres
join
Write df to Postgres

to execute:
cd /home/assamese/work/python-projects/spark-python-hl7
spark-submit --conf spark.driver.extraJavaOptions='-Dcom.amazonaws.services.s3.enableV4' --conf spark.executor.extraJavaOptions='-Dcom.amazonaws.services.s3.enableV4'   --packages org.apache.hadoop:hadoop-aws:2.7.1 --driver-class-path /home/assamese/work/postgres-jdbc/postgresql-42.2.12.jar spark_joint_pipeline_step3.py
'''

class SparkApp:

    @staticmethod
    def run(sparkContext, source_table_name1, source_table_name2, sink_table_name):
        SparkApp.logger.info(sparkContext.appName + "Starting run()")

        sqlContext = SQLContext(sparkContext)
        SparkApp.logger.info(sparkContext.appName + "Starting jdbc read() !")
        df_hl7 = sqlContext.read.jdbc(url=ConfigFramework.getPostgres_URL()
                                        , table=source_table_name1
                                        , properties=ConfigFramework.getPostgres_Properties())
        df_fhir = sqlContext.read.jdbc(url=ConfigFramework.getPostgres_URL()
                                        , table=source_table_name2
                                        , properties=ConfigFramework.getPostgres_Properties())
        SparkApp.logger.info(sparkContext.appName + "End jdbc read() !")
        print(df_hl7.show())
        print(df_fhir.show())

        df_hl7.registerTempTable("lab_obs")
        df_fhir.registerTempTable("medications")

        df_sql_result = sqlContext.sql("SELECT m.patient_id, m.resource_medicationReference_display , m.resource_authoredOn_dt FROM medications m "
                                       "JOIN (SELECT patient_id, "
                                       "CASE WHEN ob_value_noted = 'Notdetected' AND lag(ob_value_noted) "
                                       "OVER (partition by patient_id order by l.ob_dt_cleaned) = 'Detected' "
                                       "THEN 1 ELSE 0  END as qualify "
                                       "FROM lab_obs l) l ON l.patient_id = m.patient_id AND l.qualify = 1")

        # df_sql_result = sqlContext.sql("select m.resource_medicationReference_display, m.patient_id from medications m where "
        #                                "m.resource_authoredOn_dt not in (select l.ob_dt_cleaned from lab_obs l where l.patient_id = m.patient_id and l.ob_value_noted = 'Detected' ) "
        #                                )
                                       # "and "
                                       # "m.resource_authoredOn_dt < any (select l.ob_dt_cleaned from lab_obs l where l.patient_id = m.patient_id and l.ob_value_noted = 'Notdetected' )"
                                       # )
        print(df_sql_result.show())

        SparkApp.logger.info(sparkContext.appName + "Starting jdbc write() !")
        df_sql_result.write.jdbc(url=ConfigFramework.getPostgres_URL(), table=sink_table_name, mode="overwrite"
                       , properties=ConfigFramework.getPostgres_Properties())
        SparkApp.logger.info(sparkContext.appName + "End jdbc write() !")

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
    source_table_name1 = 'hl7_Pipeline_Step1_2_sink'
    source_table_name2 = 'fhir_Pipeline_Step2_1_sink'
    sink_table_name = 'joint_Pipeline_Step3_sink'
    SparkApp.run(sparkContext, source_table_name1, source_table_name2, sink_table_name)

    print("------------------------------- " + app_name + " Spark-App-end ------------------------------------------")
