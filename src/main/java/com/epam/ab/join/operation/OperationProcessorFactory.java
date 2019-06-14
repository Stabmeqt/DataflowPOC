package com.epam.ab.join.operation;

import com.epam.ab.join.cli.BasicOptions;
import com.epam.ab.join.operation.processor.*;

public class OperationProcessorFactory {

    private OperationProcessorFactory(){}

    public static OperationProcessor getProcessor(ProcessingOptions options) {
        final BasicOptions.Operation operation = options.getPipelineOptions().getOperation();
        switch (operation) {
            case JOIN: return new JoinOperationProcessor(options);
            case BATCH: return new BatchOperationProcessor(options);
            case REST:
            case GRPC: return new MicroserviceOperationProcessor(options);
            default: throw new RuntimeException("No valid operation processor found for " + operation);
        }
    }
}
