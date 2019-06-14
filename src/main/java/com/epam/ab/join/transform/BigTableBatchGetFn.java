package com.epam.ab.join.transform;

import com.epam.ab.join.model.NameYearWithRef;
import com.epam.ab.join.model.Source;
import com.epam.ab.join.model.SourceWithRef;
import com.epam.ab.join.util.ResultExtractor;
import com.google.cloud.bigtable.beam.AbstractCloudBigtableTableDoFn;
import com.google.cloud.bigtable.beam.CloudBigtableConfiguration;
import com.google.cloud.bigtable.beam.CloudBigtableScanConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BigTableBatchGetFn extends AbstractCloudBigtableTableDoFn<Source, List<SourceWithRef>> {

    private Map<String, Source> sourceMap = new HashMap<>();
    private String tableId;

    public BigTableBatchGetFn(CloudBigtableConfiguration config, String tableId) {
        super(config);
        this.tableId = tableId;
    }

    @ProcessElement
    public void process(@Element Source element, OutputReceiver<List<SourceWithRef>> receiver) {
        List<SourceWithRef> output = new ArrayList<>();
        sourceMap.put(element.getKey(),element);
        if (sourceMap.size() == 100) {
            try {
                final Table table = getConnection().getTable(TableName.valueOf(tableId));
                final List<Get> keysBatch = sourceMap.keySet().stream()
                        .map(key -> new Get(Bytes.toBytes(key)))
                        .collect(Collectors.toList());
                final Result[] result = table.get(keysBatch);
                for (Result res : result) {
                    final NameYearWithRef nameYearWithRef = ResultExtractor.extractFromResult(res);
                    output.add(new SourceWithRef(sourceMap.get(nameYearWithRef.getKey()),nameYearWithRef.getRefNum()));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            receiver.output(output);
            sourceMap.clear();
        }
    }
}
