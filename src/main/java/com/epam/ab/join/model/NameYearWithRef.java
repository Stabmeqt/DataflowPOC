package com.epam.ab.join.model;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.commons.lang3.builder.ToStringBuilder;

@DefaultCoder(AvroCoder.class)
public class NameYearWithRef extends NameYear {
    private long refNum;

    public NameYearWithRef() {
    }

    public NameYearWithRef(String name, int year, long refNum) {
        super(name, year);
        this.refNum = refNum;
    }

    public long getRefNum() {
        return refNum;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .appendSuper(super.toString())
                .append("refNum", refNum)
                .toString();
    }
}
