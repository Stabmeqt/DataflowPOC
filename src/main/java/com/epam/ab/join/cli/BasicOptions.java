package com.epam.ab.join.cli;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;

public interface BasicOptions extends DataflowPipelineOptions {

    enum Operation {
        @JsonProperty("join") JOIN,
        @JsonProperty("batch") BATCH,
        @JsonProperty("rest") REST,
        @JsonProperty("grpc") GRPC,
        @JsonProperty("datastore") DATASTORE,
        @JsonProperty("firestore") FIRESTORE
    }

    @Default.String("in/10k_c.avro")
    String getInputFile();
    void setInputFile(String inputFile);

    @Default.String("out")
    String getOutputFolder();
    void setOutputFolder(String outputFolder);

    @Default.String("instance")
    String getInstanceId();
    void setInstanceId(String instanceId);

    @Default.String("NameYear")
    String getTableId();
    void setTableId(String tableId);

    @Default.Enum("BATCH")
    Operation getOperation();
    void setOperation(Operation operation);

    @Default.String("localhost")
    String getClientHost();
    void setClientHost(String host);

    @Default.Integer(8080)
    int getClientPort();
    void setClientPort(int port);

    @Default.Long(1000L)
    long getBatchSize();
    void setBatchSize(long size);

    @Default.Boolean(false)
    boolean getFillDatastore();
    void setFillDatastore(boolean fillDatastore);
}
