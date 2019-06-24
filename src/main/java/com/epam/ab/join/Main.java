package com.epam.ab.join;

import com.epam.ab.join.cli.BasicOptions;
import com.epam.ab.join.model.DistinctNames;
import com.epam.ab.join.model.Source;
import com.epam.ab.join.model.SourceWithRef;
import com.epam.ab.join.operation.BigTableClientConnectionOptions;
import com.epam.ab.join.operation.OperationProcessorFactory;
import com.epam.ab.join.operation.ProcessingOptions;
import com.epam.ab.join.operation.processor.OperationProcessor;
import com.epam.ab.join.transform.DatastoreBatchGetFn;
import com.google.cloud.bigtable.beam.CloudBigtableScanConfiguration;
import com.google.cloud.datastore.*;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.client.DatastoreHelper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.hbase.client.Scan;

public class Main {

    public static void main(String[] args) throws Exception {
        BasicOptions pipelineOptions = null;
        try {
            pipelineOptions = PipelineOptionsFactory
                    .fromArgs(args)
                    .as(BasicOptions.class);
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            PipelineOptionsFactory.printHelp(System.err, BasicOptions.class);
            System.exit(1);
        }

        final BasicOptions.Operation operation = pipelineOptions.getOperation();
        switch (operation) {
            case REST:
            case JOIN:
            case GRPC:
            case BATCH:
                processBigTableRoutine(pipelineOptions);
                break;
            case DATASTORE:
                if (pipelineOptions.getFillDatastore()) {
                    fillDatastoreFromAvro(pipelineOptions);
                } else {
                    processDatastoreRoutine(pipelineOptions);
                }
                break;
            default:
                throw new NotImplementedException("Operation is not yet implemented");
        }
    }

    private static void processBigTableRoutine(BasicOptions pipelineOptions) {
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

    private static void processDatastoreRoutine(BasicOptions pipelineOptions) {
        final Pipeline pipeline = Pipeline.create(pipelineOptions);

        PCollection<Source> initialAvroCollection = pipeline
                .apply(AvroIO.read(Source.class).from(pipelineOptions.getInputFile()));

        initialAvroCollection
                .apply(MapElements.via(OperationProcessor.mapToKV()))
                .apply(GroupIntoBatches.ofSize(pipelineOptions.getBatchSize()))
                .apply(ParDo.of(new DatastoreBatchGetFn(pipelineOptions.getProject())))
                .apply(AvroIO.write(SourceWithRef.class)
                        .to(pipelineOptions.getOutputFolder() + "/out")
                        .withoutSharding()
                        .withSuffix(".avro"));

        pipeline.run();
    }

    private static void fillDatastoreFromAvro(BasicOptions pipelineOptions) {
        //TODO find a way to change row_number to rowNumber
        final Pipeline pipeline = Pipeline.create(pipelineOptions);

        PCollection<DistinctNames> initialAvroCollection = pipeline
                .apply(AvroIO.read(DistinctNames.class).from(pipelineOptions.getInputFile()));

        final PCollection<Entity> entityPCollection = initialAvroCollection.apply(new PTransform<PCollection<DistinctNames>, PCollection<Entity>>() {
            @Override
            public PCollection<Entity> expand(PCollection<DistinctNames> input) {
                return input.apply(MapElements.via(new SimpleFunction<DistinctNames, Entity>() {
                    @Override
                    public Entity apply(DistinctNames input) {
                        final Key.Builder testKeyBuilder = DatastoreHelper.makeKey("DistinctNames", input.getKey());
                        final Entity.Builder entityBuilder = Entity.newBuilder().setKey(testKeyBuilder.build());

                        entityBuilder.putProperties("name", DatastoreHelper.makeValue(input.getName()).build());
                        entityBuilder.putProperties("year", DatastoreHelper.makeValue(input.getYear()).build());
                        entityBuilder.putProperties("rowNumber", DatastoreHelper.makeValue(
                                input.getRowNumber()).setExcludeFromIndexes(true).build());

                        return entityBuilder.build();
                    }
                }));
            }
        });

        entityPCollection
                .apply(DatastoreIO.v1()
                        .write()
                        .withProjectId(pipelineOptions.getProject()));


        pipeline.run();
    }

    //FIXME: remove after local testing
    private static void testDatastore() throws Exception {

        final Datastore datastore = DatastoreOptions.newBuilder()
                .setHost(System.getenv("DATASTORE_EMULATOR_HOST"))
                .setProjectId(System.getenv("DATASTORE_PROJECT_ID"))
                .build().getService();

        Query<com.google.cloud.datastore.Entity> query = Query.newEntityQueryBuilder()
                .setKind("TestKind")
                .addOrderBy(StructuredQuery.OrderBy.asc("name"), StructuredQuery.OrderBy.desc("year"))
                .setLimit(100)
                .build();

        final QueryResults<com.google.cloud.datastore.Entity> queryResults = datastore.run(query);
        while (queryResults.hasNext()) {
            System.out.println(queryResults.next());
        }
    }
}
