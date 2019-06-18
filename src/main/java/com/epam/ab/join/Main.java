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

        BasicOptions pipelineOptions = null;
        try {
            pipelineOptions = PipelineOptionsFactory
                    .fromArgs(args)
                    .withValidation()
                    .as(BasicOptions.class);
        } catch (Exception e) {
            PipelineOptionsFactory.printHelp(System.err, BasicOptions.class);
            System.exit(1);
        }
        final Pipeline pipeline = Pipeline.create(pipelineOptions);


        CloudBigtableScanConfiguration configuration = null;
        BigTableClientConnectionOptions.ClientType clientType = null;
        if (pipelineOptions.getOperation() == BasicOptions.Operation.REST) {
            clientType = BigTableClientConnectionOptions.ClientType.REST;
        } else if (pipelineOptions.getOperation() == BasicOptions.Operation.GRPC) {
            clientType = BigTableClientConnectionOptions.ClientType.GRPC;
        } else {
            final Scan scan = new Scan();
            scan.setCacheBlocks(true);
            configuration = new CloudBigtableScanConfiguration.Builder()
                    .withProjectId(pipelineOptions.getProject())
                    .withInstanceId(pipelineOptions.getInstanceId())
                    .withTableId(pipelineOptions.getTableId())
                    .withScan(scan).build();
        }
        ProcessingOptions processingOptions = new ProcessingOptions(pipeline, configuration,
                pipelineOptions);

        final BigTableClientConnectionOptions connectionOptions = new BigTableClientConnectionOptions(clientType,
                pipelineOptions.getClientHost(), pipelineOptions.getClientPort());
        processingOptions.setClientConnectionOptions(connectionOptions);

        OperationProcessor processor = OperationProcessorFactory.getProcessor(processingOptions);

        processor.processPipeline();
    }
}
