package com.epam.ab.join.model;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;

@DefaultCoder(AvroCoder.class)
public class SourceWithRef extends Source implements Serializable {
    private long refNumber;

    public SourceWithRef(){
    }

    public SourceWithRef(Source source, long refNumber) {
        super(source.getState(), source.getGender(), source.getYear(), source.getName(), source.getNumber());
        this.refNumber = refNumber;
    }

    public long getRefNumber() {
        return refNumber;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .appendSuper(super.toString())
                .append("refNumber", refNumber)
                .toString();
    }
}
