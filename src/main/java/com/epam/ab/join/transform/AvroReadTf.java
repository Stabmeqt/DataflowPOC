package com.epam.ab.join.transform;

import com.epam.ab.join.model.Source;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class AvroReadTf extends PTransform<PCollection<Source>, PCollection<KV<String, Source>>> {

    @Override
    public PCollection<KV<String, Source>> expand(PCollection<Source> input) {
        return input.apply(MapElements.via(new SimpleFunction<Source, KV<String, Source>>() {
            @Override
            public KV<String, Source> apply(Source input) {
                return KV.of(input.getKey(), input);
            }
        }));
    }
}
