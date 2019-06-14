# Avro and Bigtable join on Dataflow pipeline

## Prerequisites
Dataflow API should be enabled in your GCP project.
BigTable should be pre-populated with the data from NameYear.avro (please refer https://github.com/dvalex0707/BigtableMicroservicePoc)

## Installation
Clone and compile. Fat jar will be produced.
```bash
git clone https://github.com/dvalex0707/Avro-BigTable-Join
cd Avro-BigTable-Join
mvn clean package
```
Copy the input avro file to your cloud storage
```bash
cd in
gcloud cp 10k_c.avro gs://[YOUR_BUCKET]
```

## Run the pipeline
Several parameters should be specified in order to invoke pipeline job on Dataflow: \
1. Project id
1. Runner
1. Temp staging location in cloud storage
1. Input avro file to read from (should be on cloud storage)
1. Output folder (should be on cloud storage)
1. Bigtable instance id
1. Bigtable table id
```
java -jar Avro-BigTable-Join-1.0-SNAPSHOT.jar --runner=DataflowRunner 
--tempLocation=gs://[BUCKET_ID/tmp] --project=[PROJECT_ID] --inputFile=gs://[YOUR_BUCKET]/10k_c.avro 
--outputFolder=gs://[PATH_TO_OUTPUT_FOLDER] --instanceId=[BIGTABLE_INSTANCE_ID] --tableId=[TABLE_ID]
```

## Result
As the result of the pipeline invocation, merged avro file will be created in `gs://[PATH_TO_OUTPUT_FOLDER]/out.avro`