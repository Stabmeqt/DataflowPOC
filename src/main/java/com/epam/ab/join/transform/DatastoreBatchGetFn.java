package com.epam.ab.join.transform;

import com.epam.ab.join.model.Source;
import com.epam.ab.join.model.SourceWithRef;
import com.google.cloud.datastore.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class DatastoreBatchGetFn extends DoFn<KV<String, Iterable<Source>>, SourceWithRef> {

    private Datastore datastore;
    private KeyFactory keyFactory;
    private final Map<String, Source> sourceMap = new HashMap<>();
    private final String projectId;
    public static final String KIND = "DistinctNames";

    public DatastoreBatchGetFn(String projectId) {
        this.projectId = projectId;
    }

    public synchronized Datastore getDatastore() {
        if (datastore == null) {
            datastore = DatastoreOptions.newBuilder()
                    .setProjectId(projectId)
                    .build().getService();
        }
        return datastore;
    }

    public synchronized KeyFactory getKeyFactory() {
        if (keyFactory == null) {
           keyFactory = getDatastore().newKeyFactory().setKind(KIND);
        }
        return keyFactory;
    }

    @ProcessElement
    public void process(@Element KV<String, Iterable<Source>> input, OutputReceiver<SourceWithRef> out) {
        final List<Key> keyList = StreamSupport.stream(input.getValue().spliterator(), false)
                .map(source -> {
                    sourceMap.put(source.getKey(), source);
                    return getKeyFactory().newKey(source.getKey());
                })
                .collect(Collectors.toList());

        final Iterator<Entity> queryResults = getDatastore().get(keyList);
        Iterable<Entity> iterableResult = () -> queryResults;
        StreamSupport.stream(iterableResult.spliterator(), false)
                .filter(entity -> entity != null && entity.hasKey() && entity.getKey().hasName())
                .filter(entity -> !entity.isNull("rowNumber"))
                .forEach(entity -> {
                    final Source source = sourceMap.get(entity.getKey().getName());
                    out.output(new SourceWithRef(source, entity.getLong("rowNumber")));
                });
        sourceMap.clear();
    }
}
