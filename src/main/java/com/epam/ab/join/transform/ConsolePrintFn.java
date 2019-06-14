package com.epam.ab.join.transform;

import org.apache.beam.sdk.transforms.DoFn;

public class ConsolePrintFn<T> extends DoFn<T, T> {

    @ProcessElement
    public void print(@Element T element, OutputReceiver<T> out) {
        System.out.println(element);
        out.output(element);
    }
}
