package com.epam.ab.join.operation.processor;

import com.epam.ab.join.cli.BasicOptions;
import com.epam.ab.join.model.Source;
import com.epam.ab.join.model.SourceWithRef;
import com.epam.ab.join.operation.ProcessingOptions;
import com.epam.ab.join.transform.BigTableBatchGetFn;
import com.epam.ab.join.transform.ConsolePrintFn;
import com.google.cloud.bigtable.beam.CloudBigtableScanConfiguration;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.Objects;

public class BatchOperationProcessor implements OperationProcessor {
    private final ProcessingOptions options;

    public BatchOperationProcessor(ProcessingOptions options) {
        this.options = options;
    }

    @Override
    public void processPipeline() {
        final Pipeline pipeline = options.getPipeline();
        final CloudBigtableScanConfiguration configuration = Objects.requireNonNull(options.getConfiguration());
        final BasicOptions pipelineOptions = options.getPipelineOptions();


        final PCollection<KV<String, Iterable<Source>>> batchedAvroCollection = pipeline
                .apply(AvroIO.read(Source.class).from(pipelineOptions.getInputFile()))
                .apply(MapElements.via(OperationProcessor.mapToKV()))
                .apply(GroupIntoBatches.ofSize(pipelineOptions.getBatchSize()));

        batchedAvroCollection
                .apply(ParDo.of(new BigTableBatchGetFn(configuration, pipelineOptions.getTableId())))
                .apply(Flatten.iterables())
                .apply(ParDo.of(new ConsolePrintFn<>()))
                .apply(AvroIO.write(SourceWithRef.class)
                        .to(pipelineOptions.getOutputFolder() + "/out")
                        .withoutSharding()
                        .withSuffix(".avro"));

        pipeline.run().waitUntilFinish();
    }
}
