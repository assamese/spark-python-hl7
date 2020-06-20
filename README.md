# Covid Meds

This is an attempt to track the meds used by patients who were diagnosed with Covid and subsequently recovered.

### Data

The following sample data formats are used:

* HL7 - for Lab Results
* FHIR - Prescriptions administered

### Tech

Number of open source projects are used:

* PySpark - (https://spark.apache.org/downloads.html)
* ElephantSQL - PostgreSQL as a Service (https://www.elephantsql.com/) . Fully Managed HA PostgreSQL

### How to run

Install the dependencies.

```sh
$ cd <folder that has the .py files>
$ ./run_pipeline.sh
```
### ToDos

* Ingest data from S3 (instead of local file system)
* Run the pipeline via Airflow (instead of running the shell script)
* Use S3 (parquet/avro files) for intermediate pipeline data (instead of using PostgreSQL)
