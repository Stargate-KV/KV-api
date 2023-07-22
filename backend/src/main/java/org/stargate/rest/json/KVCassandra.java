package org.stargate.rest.json;

import javax.ws.rs.core.Response;
import javax.inject.Inject;
import java.util.Map;
import java.util.Collections;

import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.api.common.cql.builder.Replication;
import io.stargate.sgv2.api.common.grpc.StargateBridgeClient;
import javax.enterprise.context.ApplicationScoped;

import java.util.List;
import java.util.ArrayList;
import io.stargate.sgv2.api.common.cql.builder.Column;
import io.stargate.sgv2.api.common.cql.builder.ImmutableColumn;

import io.stargate.sgv2.api.common.cql.builder.Predicate;

@ApplicationScoped
public class KVCassandra {
    @Inject
    StargateBridgeClient bridge;
    private static final String TABLENAME = "kvtable";

    private static cacheActive = true;

    private final int CACHE_SIZE = 1000;

    private final Map<String, String> cache = Collections.synchronizedMap(new LinkedHashMap<String, String>(CACHE_SIZE, 0.75f, true) {
        protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
            return size() > CACHE_SIZE;
        }
    });

    public void cacheOn() {
        this.cacheActive = true;
    }

    public void cacheOff() {
        this.cacheActive = false;
    }


    public Response createKeyspace(String keyspace_name) {
        QueryOuterClass.Query query = new QueryBuilder()
                .create()
                .keyspace(keyspace_name)
                .ifNotExists()
                .withReplication(Replication.simpleStrategy(1))
                .build();
        try {
            bridge.executeQuery(query);
        } catch (Exception ex) {
            // FIX: handle exception
            throw new RuntimeException(ex);
        }
        final Map<String, Object> responsePayload = Collections.singletonMap("name", keyspace_name);
        createTable(keyspace_name);
        return Response.status(Response.Status.CREATED).entity(responsePayload).build();
    }

    public void createTable(String keyspace_name) {
        List<Column> columns = new ArrayList<>();
        // build a partition key column and a value column
        ImmutableColumn.Builder key_column = ImmutableColumn.builder().name("key").type("text");
        key_column.kind(Column.Kind.PARTITION_KEY);
        ImmutableColumn.Builder val_column = ImmutableColumn.builder().name("value").type("text");
        columns.add(key_column.build());
        columns.add(val_column.build());
        QueryOuterClass.Query query = new QueryBuilder()
                .create()
                .table(keyspace_name, TABLENAME)
                .ifNotExists()
                .column(columns)
                .build();
        try {
            bridge.executeQuery(query);
        } catch (Exception ex) {
            // FIX: handle exception
            throw new RuntimeException(ex);
        }
    }

    public void putKeyVal(String keyspace_name, String key, String value) {
        // insert the value into the table
        QueryOuterClass.Query query = new QueryBuilder()
                .insertInto(keyspace_name, TABLENAME)
                .value("key", QueryOuterClass.Value.newBuilder().setString(key).build())
                .value("value", QueryOuterClass.Value.newBuilder().setString(value).build())
                .build();
        try {
            bridge.executeQuery(query);
        } catch (Exception ex) {
            // FIX: handle exception
            throw new RuntimeException(ex);
        }
        if (cacheActive) {
            cache.put(keyspace_name + key, value);
        }
    }

    public String getVal(String keyspace_name, String key) {
        if (cacheActive) {
            String value = cache.get(keyspace_name + key);
            if (value != null) {
                return value;
            }
        }
        // select the value from the table where key = key
        QueryOuterClass.Query query = new QueryBuilder()
                .select()
                .column("value")
                .from(keyspace_name, TABLENAME)
                .where("key", Predicate.EQ, QueryOuterClass.Value.newBuilder().setString(key).build())
                .build();

        QueryOuterClass.Response response;

        try {
            response = bridge.executeQuery(query);
        } catch (Exception ex) {
            // FIX: handle exception
            throw new RuntimeException(ex);
        }
        // get the row from the response
        QueryOuterClass.Row row = response.getResultSet().getRows(0);
        // get the value from the row
        String value = row.getValues(0).getString();
        if (cacheActive) {
            cache.put(keyspace_name + key, value);
        }
        return value;
    }

    public Response updateVal(String keyspace_name, String key, String value) {
        cache.put(keyspace_name + key, value);
        // update the value in the table where key = key
        QueryOuterClass.Query query = new QueryBuilder()
                .update(keyspace_name, TABLENAME)
                .value("value", QueryOuterClass.Value.newBuilder().setString(value).build())
                .where("key", Predicate.EQ, QueryOuterClass.Value.newBuilder().setString(key).build())
                .build();
        try {
            bridge.executeQuery(query);
        } catch (Exception ex) {
            // FIX: handle exception
            throw new RuntimeException(ex);
        }
        final Map<String, Object> responsePayload = Collections.singletonMap("name", keyspace_name);
        return Response.status(Response.Status.OK).entity(responsePayload).build();
    }

    public Response deleteKey(String keyspace_name, String key) {
        cache.remove(keyspace_name + key);
        // delete the row from the table where key = key
        QueryOuterClass.Query query = new QueryBuilder()
                .delete()
                .from(keyspace_name, TABLENAME)
                .where("key", Predicate.EQ, QueryOuterClass.Value.newBuilder().setString(key).build())
                .build();
        try {
            bridge.executeQuery(query);
        } catch (Exception ex) {
            // FIX: handle exception
            throw new RuntimeException(ex);
        }
        final Map<String, Object> responsePayload = Collections.singletonMap("name", keyspace_name);
        return Response.status(Response.Status.OK).entity(responsePayload).build();
    }

    public Response deleteKeyspace(String keyspace_name) {
        cache.clear();
        QueryOuterClass.Query query = new QueryBuilder()
                .drop()
                .keyspace(keyspace_name)
                .build();
        try {
            bridge.executeQuery(query);
        } catch (Exception ex) {
            // FIX: handle exception
            throw new RuntimeException(ex);
        }
        final Map<String, Object> responsePayload = Collections.singletonMap("name", keyspace_name);
        return Response.status(Response.Status.OK).entity(responsePayload).build();
    }
}

