package com.epam.ab.join.transform;

import com.company.bigtablemicroservicepoc.BigTableClient;
import com.company.bigtablemicroservicepoc.BigTableGrpcClient;
import com.company.bigtablemicroservicepoc.BigTableRestClient;
import com.company.bigtablemicroservicepoc.NameYear;
import com.epam.ab.join.model.Source;
import com.epam.ab.join.model.SourceWithRef;
import com.epam.ab.join.operation.BigTableClientConnectionOptions;
import org.apache.beam.sdk.transforms.DoFn;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class BigTableClientFn extends DoFn<Source, List<SourceWithRef>> {

    private List<Source> buffer = new ArrayList<>();
    private BigTableClient client;
    private BigTableClientConnectionOptions connectionOptions;


    public BigTableClientFn(BigTableClientConnectionOptions connectionOptions) {
        this.connectionOptions = connectionOptions;
    }

    public synchronized BigTableClient getClient() {
        if (client == null) {
            if (connectionOptions.getClientType() == BigTableClientConnectionOptions.ClientType.REST) {
                client = new BigTableRestClient(connectionOptions.getClientHost(), connectionOptions.getClientPort());
            } else {
                client = new BigTableGrpcClient(connectionOptions.getClientHost(), connectionOptions.getClientPort());
            }
        }
        return client;
    }

    @ProcessElement
    public void processElement(@Element Source element, OutputReceiver<List<SourceWithRef>> receiver) {
        buffer.add(element);
        if (buffer.size() == 100) {

            List<NameYear> nameYearList = buffer.stream().map(source -> new NameYear(source.getName(), (int) source.getYear()))
                    .collect(Collectors.toList());
            List<Long> refNumbers = getClient().getRefNumbers(nameYearList);
            List<SourceWithRef> resultList = new ArrayList<>();
            for (int i = 0; i < buffer.size(); i++) {
                resultList.add(new SourceWithRef(buffer.get(i), refNumbers.get(i)));
            }

            receiver.output(resultList);
            buffer.clear();
        }
    }
}
