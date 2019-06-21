package com.epam.ab.join;

import com.epam.ab.join.cli.BasicOptions;
import com.epam.ab.join.model.DistinctNames;
import com.epam.ab.join.operation.BigTableClientConnectionOptions;
import com.epam.ab.join.operation.OperationProcessorFactory;
import com.epam.ab.join.operation.ProcessingOptions;
import com.epam.ab.join.operation.processor.OperationProcessor;
import com.epam.ab.join.transform.ConsolePrintFn;
import com.google.cloud.bigtable.beam.CloudBigtableScanConfiguration;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.KeyFactory;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.client.DatastoreHelper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.hbase.client.Scan;

public class Main {

    private static long id;
    private static String dsEmHost;

    public static void main(String[] args) throws Exception{

        /*LocalDatastoreHelper helper = LocalDatastoreHelper.create();
        helper.start();
        dsEmHost = "localhost:" + helper.getPort();
        final String projectId = helper.getProjectId();
        System.out.println("[Datastore-Emulator] listening on : " + dsEmHost);
        System.setProperty("DATASTORE_EMULATOR_HOST",dsEmHost);*/

        final String projectId = System.getenv("DATASTORE_PROJECT_ID");

        //processBigTableRoutine(args);
        processDatastoreRoutine(args, projectId);
        testDatastore(projectId);
    }

    private static void processBigTableRoutine(String[] args) {
        BasicOptions pipelineOptions = null;
        try {
            pipelineOptions = PipelineOptionsFactory
                    .fromArgs(args)
                    .withValidation()
                    .as(BasicOptions.class);
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            PipelineOptionsFactory.printHelp(System.err, BasicOptions.class);
            System.exit(1);
        }
        final Pipeline pipeline = Pipeline.create(pipelineOptions);


        CloudBigtableScanConfiguration configuration = null;
        BigTableClientConnectionOptions.ClientType clientType = null;
        if (pipelineOptions.getOperation() == BasicOptions.Operation.REST) {
            clientType = BigTableClientConnectionOptions.ClientType.REST;
        } else if (pipelineOptions.getOperation() == BasicOptions.Operation.GRPC) {
            clientType = BigTableClientConnectionOptions.ClientType.GRPC;
        } else {
            final Scan scan = new Scan();
            scan.setCacheBlocks(true);
            configuration = new CloudBigtableScanConfiguration.Builder()
                    .withProjectId(pipelineOptions.getProject())
                    .withInstanceId(pipelineOptions.getInstanceId())
                    .withTableId(pipelineOptions.getTableId())
                    .withScan(scan).build();
        }
        ProcessingOptions processingOptions = new ProcessingOptions(pipeline, configuration,
                pipelineOptions);

        final BigTableClientConnectionOptions connectionOptions = new BigTableClientConnectionOptions(clientType,
                pipelineOptions.getClientHost(), pipelineOptions.getClientPort());
        processingOptions.setClientConnectionOptions(connectionOptions);

        OperationProcessor processor = OperationProcessorFactory.getProcessor(processingOptions);

        processor.processPipeline();
    }

    private static void processDatastoreRoutine(String[] args, String projectId) throws Exception {
        //TODO find a way to change row_number to rowNumber
        final Pipeline pipeline = Pipeline.create();

        PCollection<DistinctNames> initialAvroCollection = pipeline
                .apply(AvroIO
                        .read(DistinctNames.class).from("in/new_names.avro"))
                .apply(Sample.any(10L));

        initialAvroCollection
                .apply(Count.globally())
                .apply(ParDo.of(new ConsolePrintFn<>()));

        final PCollection<Entity> entityPCollection = initialAvroCollection.apply(new PTransform<PCollection<DistinctNames>, PCollection<Entity>>() {
            @Override
            public PCollection<Entity> expand(PCollection<DistinctNames> input) {
                final PCollection<Entity> entityPCollection = input.apply(MapElements.via(new SimpleFunction<DistinctNames, Entity>() {
                    @Override
                    public Entity apply(DistinctNames input) {
                        final Key.Builder testKeyBuilder = DatastoreHelper.makeKey("TestKind", input.getRowNumber());
                        final Entity.Builder entityBuilder = Entity.newBuilder().setKey(testKeyBuilder.build());

                        entityBuilder.putProperties("name", DatastoreHelper.makeValue(input.getName()).build());
                        entityBuilder.putProperties("year", DatastoreHelper.makeValue(input.getYear()).build());
                        entityBuilder.putProperties("rowNumber", DatastoreHelper.makeValue(input.getRowNumber()).setExcludeFromIndexes(true).build());

                        id = input.getRowNumber();

                        return entityBuilder.build();
                    }
                }));
                entityPCollection.apply(ParDo.of(new ConsolePrintFn<>()));
                return entityPCollection;
            }
        });

        DatastoreV1.Write datastoreWrite = DatastoreIO.v1()
                .write()
                .withLocalhost(System.getenv("DATASTORE_EMULATOR_HOST"))
                .withProjectId(projectId);

        entityPCollection.apply(datastoreWrite);

        pipeline.run().waitUntilFinish();
    }

    private static void testDatastore(String projectId) throws Exception {

        final Datastore datastore = DatastoreOptions.newBuilder()
                .setHost(System.getenv("DATASTORE_EMULATOR_HOST"))
                .setProjectId(projectId)
                .build().getService();

        KeyFactory keyFactory = datastore.newKeyFactory().setKind("TestKind");

        final com.google.cloud.datastore.Entity entity = datastore.get(keyFactory.newKey(id));
        System.out.println("Found: " + entity);
    }
}
