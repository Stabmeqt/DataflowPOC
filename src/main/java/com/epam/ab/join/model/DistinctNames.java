package com.epam.ab.join.model;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.commons.lang3.builder.ToStringBuilder;

@DefaultCoder(AvroCoder.class)
public class DistinctNames {

    private String name;
    private long year;
    private long row_number;

    public String getName() {
        return name;
    }

    public long getYear() {
        return year;
    }

    public long getRowNumber() {
        return row_number;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("name", name)
                .append("year", year)
                .append("rowNumber", row_number)
                .toString();
    }
}
