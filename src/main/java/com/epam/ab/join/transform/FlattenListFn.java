package com.epam.ab.join.transform;

import org.apache.beam.sdk.transforms.DoFn;

import java.util.List;

public class FlattenListFn<T> extends DoFn<List<T>, T> {
    @ProcessElement
    public void flatten(@Element List<T> input, OutputReceiver<T> receiver) {
        input.forEach(receiver::output);
    }
}
