package com.epam.ab.join;

import com.epam.ab.join.cli.BasicOptions;
import com.epam.ab.join.model.NameYearWithRef;
import com.epam.ab.join.model.Source;
import com.epam.ab.join.model.SourceWithRef;
import com.epam.ab.join.transform.BigTableBatchGetFn;
import com.epam.ab.join.transform.ConsolePrintFn;
import com.epam.ab.join.util.ResultExtractor;
import com.google.cloud.bigtable.beam.AbstractCloudBigtableTableDoFn;
import com.google.cloud.bigtable.beam.CloudBigtableScanConfiguration;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MainBatch {

    public static void main(String[] args) {

        final BasicOptions options = PipelineOptionsFactory.fromArgs(args).as(BasicOptions.class);
        final Pipeline pipeline = Pipeline.create(options);

        final Scan scan = new Scan();
        scan.setCacheBlocks(true);

        final String tableId = options.getTableId();

        CloudBigtableScanConfiguration configuration = new CloudBigtableScanConfiguration.Builder()
                .withProjectId(options.getProject())
                .withInstanceId(options.getInstanceId())
                .withTableId(tableId)
                .withScan(scan).build();

        final PCollection<Source> avroCollection =
                pipeline.apply(AvroIO.read(Source.class).from(options.getInputFile()));

        avroCollection
                .apply(ParDo.of(new BigTableBatchGetFn(configuration,tableId)))
                .apply(ParDo.of(new ConsolePrintFn<>()));

        pipeline.run().waitUntilFinish();
    }
}
