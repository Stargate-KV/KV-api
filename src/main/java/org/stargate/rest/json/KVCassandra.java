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
    private static final String TABLENAME = "kvstore";

    public Response createKeyspace(int db_id) {
        QueryOuterClass.Query query = new QueryBuilder()
                .create()
                // convert db_id to string and use it as keyspace name
                .keyspace(Integer.toString(db_id))
                .ifNotExists()
                .withReplication(Replication.simpleStrategy(1))
                .build();
        try {
            bridge.executeQuery(query);
        } catch (Exception ex) {
            // FIX: handle exception
            throw new RuntimeException(ex);
        }
        final Map<String, Object> responsePayload = Collections.singletonMap("name", Integer.toString(db_id));
        createTable(Integer.toString(db_id));
        return Response.status(Response.Status.CREATED).entity(responsePayload).build();
    }

    public void createTable(String keyspace_name) {
        List<Column> columns = new ArrayList<>();
        // build a partition key column
        ImmutableColumn.Builder column = ImmutableColumn.builder().name("partition_key").type("text");
        column.kind(Column.Kind.PARTITION_KEY);
        columns.add(column.build());
        // build a string type column named "kv_string"
        // ImmutableColumn.Builder column =
        // ImmutableColumn.builder().name(COLNAME).type("text");
        // columns.add(column.build());
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
        // add a column named key in the table and insert value
        ImmutableColumn.Builder column = ImmutableColumn.builder().name(key).type("text");
        // add the column to the table
        // if table TABLENAME does not exist, create it
        QueryOuterClass.Query query = new QueryBuilder()
                .alter()
                .table(keyspace_name, TABLENAME)
                .addColumn(column.build())
                .build();
        try {
            bridge.executeQuery(query);
        } catch (Exception ex) {
            // FIX: handle exception
            throw new RuntimeException(ex);
        }

        // insert the value into the table
        query = new QueryBuilder()
                .insertInto(keyspace_name, TABLENAME)
                .value("partition_key", QueryOuterClass.Value.newBuilder().setString("default").build())
                .value(key, QueryOuterClass.Value.newBuilder().setString(value).build())
                .build();
        try {
            bridge.executeQuery(query);
        } catch (Exception ex) {
            // FIX: handle exception
            throw new RuntimeException(ex);
        }
    }

    public String getVal(String keyspace_name, String key) {
        String col_name = key;
        // select the column named key from the table and get the first row
        QueryOuterClass.Query query = new QueryBuilder()
                .select()
                .column(col_name)
                .from(keyspace_name, TABLENAME)
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
        return value;
    }
}

