package com.epam.ab.join.transform;

import com.epam.ab.join.model.NameYearWithRef;
import com.epam.ab.join.util.ResultExtractor;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class BigTableReadTf extends PTransform<PCollection<Result>, PCollection<KV<String, NameYearWithRef>>> {

    @Override
    public PCollection<KV<String, NameYearWithRef>> expand(PCollection<Result> input) {
        final PCollection<NameYearWithRef> initialRead = input.apply(
                MapElements.via(new SimpleFunction<Result, NameYearWithRef>() {
                    @Override
                    public NameYearWithRef apply(Result input) {
                        return ResultExtractor.extractFromResult(input);
                    }
                })
        );
        return initialRead.apply(MapElements.via(new SimpleFunction<NameYearWithRef, KV<String, NameYearWithRef>>() {
            @Override
            public KV<String, NameYearWithRef> apply(NameYearWithRef input) {
                return KV.of(input.getKey(), input);
            }
        }));
    }
}
