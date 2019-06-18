package com.epam.ab.join.model;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.commons.lang3.builder.ToStringBuilder;

@DefaultCoder(AvroCoder.class)
public class NameYear {
    private String name;
    private int year;

    public NameYear() {
    }

    public NameYear(String name, int year) {
        this.name = name;
        this.year = year;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public String getKey() {
        return String.format("%s|%d", name, year);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("name", name)
                .append("year", year)
                .toString();
    }
}
