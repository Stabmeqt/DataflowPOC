package com.epam.ab.join.operation;

import com.epam.ab.join.cli.BasicOptions;
import com.google.cloud.bigtable.beam.CloudBigtableScanConfiguration;
import org.apache.beam.sdk.Pipeline;

import java.util.Objects;

public class ProcessingOptions {
    private Pipeline pipeline;
    private CloudBigtableScanConfiguration configuration;
    private BasicOptions pipelineOptions;
    private BigTableClientConnectionOptions clientConnectionOptions;

    public ProcessingOptions(Pipeline pipeline, CloudBigtableScanConfiguration configuration,
                             BasicOptions pipelineOptions) {
        this.pipeline = Objects.requireNonNull(pipeline);
        this.configuration = Objects.requireNonNull(configuration);
        this.pipelineOptions = Objects.requireNonNull(pipelineOptions);
    }

    public BigTableClientConnectionOptions getClientConnectionOptions() {
        return clientConnectionOptions;
    }

    public void setClientConnectionOptions(BigTableClientConnectionOptions clientConnectionOptions) {
        this.clientConnectionOptions = clientConnectionOptions;
    }

    public Pipeline getPipeline() {
        return pipeline;
    }

    public CloudBigtableScanConfiguration getConfiguration() {
        return configuration;
    }

    public BasicOptions getPipelineOptions() {
        return pipelineOptions;
    }
}
