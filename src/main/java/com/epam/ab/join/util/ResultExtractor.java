package com.epam.ab.join.util;

import com.epam.ab.join.model.NameYearWithRef;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class ResultExtractor {

    private static final byte[] FAMILY = Bytes.toBytes("cf1");
    private static final byte[] NAME_QUALIFIER = Bytes.toBytes("name");
    private static final byte[] YEAR_QUALIFIER = Bytes.toBytes("year");
    private static final byte[] REFNUM_QUALIFIER = Bytes.toBytes("refnumber");

    private ResultExtractor(){};

    public static NameYearWithRef extractFromResult(Result input) {
        NameYearWithRef transformationResult = null;

        final boolean nameExists = input.containsNonEmptyColumn(FAMILY, NAME_QUALIFIER);
        final boolean yearExists = input.containsNonEmptyColumn(FAMILY, YEAR_QUALIFIER);

        if (nameExists && yearExists) {
            final byte[] name = input.getValue(FAMILY, NAME_QUALIFIER);
            final byte[] year = input.getValue(FAMILY, YEAR_QUALIFIER);
            final byte[] refNum = input.getValue(FAMILY, REFNUM_QUALIFIER);
            transformationResult = new NameYearWithRef(
                    Bytes.toStringBinary(name),
                    Bytes.toInt(year),
                    Bytes.toLong(refNum));
        }
        return transformationResult;
    }
}
