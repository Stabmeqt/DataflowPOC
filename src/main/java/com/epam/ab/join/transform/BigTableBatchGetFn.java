package com.epam.ab.join.transform;

import com.epam.ab.join.model.Source;
import com.epam.ab.join.model.SourceWithRef;
import com.epam.ab.join.util.ResultExtractor;
import com.google.cloud.bigtable.beam.AbstractCloudBigtableTableDoFn;
import com.google.cloud.bigtable.beam.CloudBigtableConfiguration;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class BigTableBatchGetFn extends AbstractCloudBigtableTableDoFn<KV<String, Iterable<Source>>, List<SourceWithRef>> {

    private String tableId;
    private final Map<String, Source> sourceMap = new HashMap<>();

    public BigTableBatchGetFn(CloudBigtableConfiguration config, String tableId) {
        super(config);
        this.tableId = tableId;
    }

    @ProcessElement
    public void process(@Element KV<String, Iterable<Source>> element, OutputReceiver<List<SourceWithRef>> receiver) {
        final Iterable<Source> values = element.getValue();
        final List<Get> keysBatch = StreamSupport.stream(values.spliterator(), false)
                .map(source -> {
                    sourceMap.put(source.getKey(), source);
                    return new Get(Bytes.toBytes(source.getKey()));
                })
                .collect(Collectors.toList());
        Table table;
        Result[] result = null;
        try {
            table = getConnection().getTable(TableName.valueOf(tableId));
            result = table.get(keysBatch);
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (result != null) {
            final List<SourceWithRef> joined = Stream.of(result)
                    .map(item -> ResultExtractor.extractFromResult(item))
                    .filter(extracted -> extracted != null
                            && extracted.getKey() != null
                            && sourceMap.get(extracted.getKey()) != null)
                    .map(extracted -> new SourceWithRef(sourceMap.get(extracted.getKey()), extracted.getRefNum()))
                    .collect(Collectors.toList());
            receiver.output(joined);
            sourceMap.clear();
        }
    }
}
