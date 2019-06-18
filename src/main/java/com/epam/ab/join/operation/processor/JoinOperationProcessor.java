package com.epam.ab.join.operation.processor;

import com.epam.ab.join.cli.BasicOptions;
import com.epam.ab.join.model.NameYearWithRef;
import com.epam.ab.join.model.Source;
import com.epam.ab.join.model.SourceWithRef;
import com.epam.ab.join.operation.ProcessingOptions;
import com.epam.ab.join.transform.AvroReadTf;
import com.epam.ab.join.transform.BigTableReadTf;
import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableScanConfiguration;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.joinlibrary.Join;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class JoinOperationProcessor implements OperationProcessor {
    private final ProcessingOptions options;

    public JoinOperationProcessor(ProcessingOptions options) {
        this.options = options;
    }

    private static SimpleFunction<KV<String, KV<Source, Long>>, SourceWithRef> mapToSourceWithRef() {
        return new SimpleFunction<KV<String, KV<Source, Long>>, SourceWithRef>() {
            @Override
            public SourceWithRef apply(KV<String, KV<Source, Long>> input) {
                final KV<Source, Long> value = input.getValue();
                return new SourceWithRef(value.getKey(), value.getValue());
            }
        };
    }

    @Override
    public void processPipeline() {
        final Pipeline pipeline = options.getPipeline();
        final CloudBigtableScanConfiguration configuration = options.getConfiguration();
        final BasicOptions pipelineOptions = options.getPipelineOptions();

        final PCollection<KV<String, Long>> kvBigTableCollection =
                pipeline
                        .apply(Read.from(CloudBigtableIO.read(configuration)))
                        .apply(new BigTableReadTf()).apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(),
                        TypeDescriptors.longs()))
                        .via((KV<String, NameYearWithRef> kv) -> KV.of(kv.getKey(), kv.getValue().getRefNum())
                        ));

        final PCollection<KV<String, Source>> avroCollection = pipeline
                .apply(AvroIO.read(Source.class).from(pipelineOptions.getInputFile()))
                .apply(new AvroReadTf());

        final PCollection<KV<String, KV<Source, Long>>> joined = Join.innerJoin(avroCollection, kvBigTableCollection);

        joined
                .apply(MapElements.via(mapToSourceWithRef()))
                .apply(AvroIO.write(SourceWithRef.class)
                        .to(pipelineOptions.getOutputFolder() + "/out")
                        .withoutSharding()
                        .withSuffix(".avro"));

        pipeline.run();
    }
}
