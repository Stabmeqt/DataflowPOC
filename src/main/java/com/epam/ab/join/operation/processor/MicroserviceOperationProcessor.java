package com.epam.ab.join.operation.processor;

import com.epam.ab.join.cli.BasicOptions;
import com.epam.ab.join.model.Source;
import com.epam.ab.join.model.SourceWithRef;
import com.epam.ab.join.operation.BigTableClientConnectionOptions;
import com.epam.ab.join.operation.ProcessingOptions;
import com.epam.ab.join.transform.BigTableClientFn;
import com.epam.ab.join.transform.ConsolePrintFn;
import com.epam.ab.join.transform.FlattenListFn;
import com.google.cloud.bigtable.beam.CloudBigtableScanConfiguration;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.transforms.ParDo;
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
        final PCollection<Source> avroCollection =
                pipeline.apply(AvroIO.read(Source.class).from(pipelineOptions.getInputFile()));

        avroCollection
                .apply(ParDo.of(new BigTableClientFn(options.getClientConnectionOptions())))
                .apply(ParDo.of(new FlattenListFn<>()))
                .apply(ParDo.of(new ConsolePrintFn<>()))
                .apply(AvroIO.write(SourceWithRef.class)
                        .to(pipelineOptions.getOutputFolder() + "/out")
                        .withoutSharding()
                        .withSuffix(".avro"));

        pipeline.run().waitUntilFinish();
    }
}
