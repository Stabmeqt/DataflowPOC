package com.epam.ab.join;

import com.epam.ab.join.cli.BasicOptions;
import com.epam.ab.join.operation.BigTableClientConnectionOptions;
import com.epam.ab.join.operation.OperationProcessorFactory;
import com.epam.ab.join.operation.ProcessingOptions;
import com.epam.ab.join.operation.processor.OperationProcessor;
import com.google.cloud.bigtable.beam.CloudBigtableScanConfiguration;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.hadoop.hbase.client.Scan;

public class Main {

    public static void main(String[] args) {

        final BasicOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).as(BasicOptions.class);
        final Pipeline pipeline = Pipeline.create(pipelineOptions);

        Scan scan = new Scan();
        scan.setCacheBlocks(true);

        CloudBigtableScanConfiguration configuration = new CloudBigtableScanConfiguration.Builder()
                .withProjectId(pipelineOptions.getProject())
                .withInstanceId(pipelineOptions.getInstanceId())
                .withTableId(pipelineOptions.getTableId())
                .withScan(scan).build();

        final ProcessingOptions processingOptions = new ProcessingOptions(pipeline, configuration,
                pipelineOptions);

        BigTableClientConnectionOptions.ClientType clientType = null;
        if (pipelineOptions.getOperation() == BasicOptions.Operation.REST) {
            clientType = BigTableClientConnectionOptions.ClientType.REST;
        } else if (pipelineOptions.getOperation() == BasicOptions.Operation.GRPC) {
            clientType = BigTableClientConnectionOptions.ClientType.GRPC;
        }

        final BigTableClientConnectionOptions connectionOptions = new BigTableClientConnectionOptions(clientType,
                pipelineOptions.getClientHost(), pipelineOptions.getClientPort());
        processingOptions.setClientConnectionOptions(connectionOptions);

        OperationProcessor processor = OperationProcessorFactory.getProcessor(processingOptions);

        processor.processPipeline();
    }
}
