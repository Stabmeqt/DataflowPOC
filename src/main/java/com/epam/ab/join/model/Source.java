package com.epam.ab.join.model;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.commons.lang3.builder.ToStringBuilder;

@DefaultCoder(AvroCoder.class)
public class Source {
  private String state;
  private String gender;
  private long year;
  private String name;
  private long number;

  public Source() {
  }

  public Source(String state, String gender, long year, String name, long number) {
    this.state = state;
    this.gender = gender;
    this.year = year;
    this.name = name;
    this.number = number;
  }

  public String getState() {
    return state;
  }

  public String getGender() {
    return gender;
  }

  public long getYear() {
    return year;
  }

  public String getName() {
    return name;
  }

  public long getNumber() {
    return number;
  }

  public String getKey() {
    return String.format("%s|%d", name, year);
  }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("state", state)
                .append("gender", gender)
                .append("year", year)
                .append("name", name)
                .append("number", number)
                .toString();
    }
}
