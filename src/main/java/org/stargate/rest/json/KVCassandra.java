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
import java.util.stream.Collectors;

import io.stargate.sgv2.api.common.cql.builder.Predicate;

@ApplicationScoped
public class KVCassandra {
    private static final String TABLE_NAME = "table_name";
    private static final String SYSTEM_SCHEMA = "system_schema";
    private static final String TABLES = "tables";
    private static final String KEYSPACE_NAME_COLUMN = "keyspace_name";
    @Inject
    StargateBridgeClient bridge;

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
        return Response.status(Response.Status.CREATED).entity(responsePayload).build();
    }

    public Response deleteKeyspace(String keyspace_name) {
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

    public void createTable(String keyspace_name, String table_name) {
        List<Column> columns = new ArrayList<>();
        // build a partition key column and a value column
        ImmutableColumn.Builder key_column = ImmutableColumn.builder().name("key").type("text");
        key_column.kind(Column.Kind.PARTITION_KEY);
        ImmutableColumn.Builder val_column = ImmutableColumn.builder().name("value").type("text");
        columns.add(key_column.build());
        columns.add(val_column.build());
        QueryOuterClass.Query query = new QueryBuilder()
                .create()
                .table(keyspace_name, table_name)
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

    public Response deleteTable(String keyspace_name, String table_name) {
        QueryOuterClass.Query query = new QueryBuilder()
                .drop()
                .table(keyspace_name, table_name)
                .build();
        try {
            bridge.executeQuery(query);
        } catch (Exception ex) {
            // FIX: handle exception
            throw new RuntimeException(ex);
        }
        final Map<String, Object> responsePayload = Collections.singletonMap("name", table_name);
        return Response.status(Response.Status.OK).entity(responsePayload).build();
    }

    public Response listKeyspaces() {
        // list all keyspaces in the database
        QueryBuilder.QueryBuilder__21 queryBuilder = new QueryBuilder()
                .select()
                .column(KEYSPACE_NAME_COLUMN)
                .from(SYSTEM_SCHEMA, "keyspaces");
        QueryOuterClass.Response response;
        try {
            response = bridge.executeQuery(queryBuilder.build());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        List<String> allKeyspaceNames = response.getResultSet().getRowsList().stream()
                .map(row -> row.getValues(0))
                .map(QueryOuterClass.Value::getString)
                .collect(Collectors.toList());
        return Response.status(Response.Status.OK).entity(allKeyspaceNames).build();
    }

    public Response listTables(String keyspace_name) {
        // list all tables in the keyspace
        QueryBuilder.QueryBuilder__21 queryBuilder = new QueryBuilder()
                .select()
                .column(TABLE_NAME)
                .from(SYSTEM_SCHEMA, TABLES)
                .where(
                        KEYSPACE_NAME_COLUMN,
                        Predicate.EQ,
                        QueryOuterClass.Value.newBuilder().setString(keyspace_name).build());
        QueryOuterClass.Response response;
        try {
            response = bridge.executeQuery(queryBuilder.build());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        List<String> allTableNames = response.getResultSet().getRowsList().stream()
                .map(row -> row.getValues(0))
                .map(QueryOuterClass.Value::getString)
                .collect(Collectors.toList());
        return Response.status(Response.Status.OK).entity(allTableNames).build();
    }

    public void putKeyVal(String keyspace_name, String table_name, String key, String value) {
        // insert the value into the table
        QueryOuterClass.Query query = new QueryBuilder()
                .insertInto(keyspace_name, table_name)
                .value("key", QueryOuterClass.Value.newBuilder().setString(key).build())
                .value("value", QueryOuterClass.Value.newBuilder().setString(value).build())
                .build();
        try {
            bridge.executeQuery(query);
        } catch (Exception ex) {
            // FIX: handle exception
            throw new RuntimeException(ex);
        }
    }

    public String getVal(String keyspace_name, String table_name, String key) {
        // select the value from the table where key = key
        QueryOuterClass.Query query = new QueryBuilder()
                .select()
                .column("value")
                .from(keyspace_name, table_name)
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
        return value;
    }

    public Response updateVal(String keyspace_name, String table_name, String key, String value) {
        // update the value in the table where key = key, if it exists, otherwise return
        // 404
        // TODO: check if the key exists
        QueryOuterClass.Query query = new QueryBuilder()
                .update(keyspace_name, table_name)
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

    public Response deleteKey(String keyspace_name, String table_name, String key) {
        // delete the row from the table where key = key
        QueryOuterClass.Query query = new QueryBuilder()
                .delete()
                .from(keyspace_name, table_name)
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

}

