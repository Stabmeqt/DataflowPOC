package com.epam.ab.join.transform;

import com.company.bigtablemicroservicepoc.BigTableClient;
import com.company.bigtablemicroservicepoc.BigTableGrpcClient;
import com.company.bigtablemicroservicepoc.BigTableRestClient;
import com.company.bigtablemicroservicepoc.NameYear;
import com.epam.ab.join.model.Source;
import com.epam.ab.join.model.SourceWithRef;
import com.epam.ab.join.operation.BigTableClientConnectionOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class BigTableClientFn extends DoFn<KV<String, Iterable<Source>>, List<SourceWithRef>> {

    private final BigTableClientConnectionOptions connectionOptions;
    private BigTableClient client;

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
    public void processElement(
            @Element KV<String, Iterable<Source>> element, OutputReceiver<List<SourceWithRef>> receiver) {
        final List<Source> sources = StreamSupport.stream(element.getValue().spliterator(), false)
                .collect(Collectors.toList());
        final List<NameYear> nameYearList = sources.stream()
                .map(source -> new NameYear(source.getName(), (int) source.getYear()))
                .collect(Collectors.toList());

        final List<Long> refNumbers = getClient().getRefNumbers(nameYearList);
        List<SourceWithRef> resultList = new ArrayList<>();

        for (int i = 0; i < sources.size(); i++) {
            if (refNumbers.get(i) != null) {
                resultList.add(new SourceWithRef(sources.get(i), refNumbers.get(i)));
            }
        }

        receiver.output(resultList);
    }
}
