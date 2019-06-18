package com.epam.ab.join.operation.processor;

import com.epam.ab.join.cli.BasicOptions;
import com.epam.ab.join.model.Source;
import com.epam.ab.join.model.SourceWithRef;
import com.epam.ab.join.operation.ProcessingOptions;
import com.epam.ab.join.transform.BigTableClientFn;
import com.epam.ab.join.transform.FlattenListFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class MicroserviceOperationProcessor implements OperationProcessor {
    private final ProcessingOptions options;

    public MicroserviceOperationProcessor(ProcessingOptions options) {
        this.options = options;
    }

    @Override
    public void processPipeline() {
        final Pipeline pipeline = options.getPipeline();
        final BasicOptions pipelineOptions = options.getPipelineOptions();

        final PCollection<KV<String, Iterable<Source>>> batchedAvroCollection = pipeline.apply(AvroIO.read(Source.class).from(pipelineOptions.getInputFile()))
                .apply(MapElements.via(OperationProcessor.mapToKV()))
                .apply(GroupIntoBatches.ofSize(pipelineOptions.getBatchSize()));

        batchedAvroCollection
                .apply(ParDo.of(new BigTableClientFn(options.getClientConnectionOptions())))
                .apply(Flatten.iterables())
                .apply(AvroIO.write(SourceWithRef.class)
                        .to(pipelineOptions.getOutputFolder() + "/out")
                        .withoutSharding()
                        .withSuffix(".avro"));

        pipeline.run();
    }
}
