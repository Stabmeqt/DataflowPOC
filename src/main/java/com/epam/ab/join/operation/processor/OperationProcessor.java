package com.epam.ab.join.operation.processor;

import com.epam.ab.join.model.Source;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;

public interface OperationProcessor {

    void processPipeline();

    static SimpleFunction<Source, KV<String, Source>> mapToKV() {
        return new SimpleFunction<Source, KV<String, Source>>() {
            @Override
            public KV<String, Source> apply(Source input) {
                return KV.of(input.getState(), input);
            }
        };
    }
}
