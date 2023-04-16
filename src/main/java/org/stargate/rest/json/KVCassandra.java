package org.stargate.rest.json;

import javax.ws.rs.core.Response;
import javax.inject.Inject;
import javax.naming.InvalidNameException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

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

    public KVResponse createKeyspace(String keyspace_name) {
        // create keyspace
        QueryOuterClass.Query query_create = new QueryBuilder()
                .create()
                .keyspace(keyspace_name)
                .withReplication(Replication.simpleStrategy(1))
                .build();

        try {
            bridge.executeQuery(query_create);
        } catch (StatusRuntimeException ex) {
            // handle exception when Status is INVALID_ARGUMENT
            return new KVResponse(ex.getStatus().getCode().value(), ex.getMessage());
        } catch (Exception ex) {
            return new KVResponse(500, ex.getMessage());

        }
        return new KVResponse(201,
                "The database '" + keyspace_name + "' has been created successfully.");
    }

    public KVResponse deleteKeyspace(String keyspace_name) {
        QueryOuterClass.Query query = new QueryBuilder()
                .drop()
                .keyspace(keyspace_name)
                .build();
        try {
            bridge.executeQuery(query);
        } catch (StatusRuntimeException ex) {
            // handle exception when Status is INVALID_ARGUMENT
            if (ex.getStatus().getCode() == Status.Code.INVALID_ARGUMENT) {
                return new KVResponse(404, ex.getMessage());
            }
        } catch (Exception ex) {
            return new KVResponse(500, ex.getMessage());
        }
        return new KVResponse(200,
                "The database '" + keyspace_name + "' has been deleted successfully.");
    }

    public KVResponse createTable(String keyspace_name, String table_name) {
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
                .column(columns)
                .build();
        try {
            bridge.executeQuery(query);
        } catch (StatusRuntimeException ex) {
            return new KVResponse(ex.getStatus().getCode().value(), ex.getMessage());
        } catch (Exception ex) {
            return new KVResponse(500, ex.getMessage());
        }
        return new KVResponse(201,
                "The table '" + table_name + "' has been created successfully.");
    }

    public KVResponse deleteTable(String keyspace_name, String table_name) {
        QueryOuterClass.Query query = new QueryBuilder()
                .drop()
                .table(keyspace_name, table_name)
                .build();
        try {
            bridge.executeQuery(query);
        } catch (StatusRuntimeException ex) {
            // handle exception when Status is INVALID_ARGUMENT
            return new KVResponse(ex.getStatus().getCode().value(), ex.getMessage());
        } catch (Exception ex) {
            return new KVResponse(500, ex.getMessage());
        }
        return new KVResponse(200,
                "The table '" + table_name + "' has been deleted successfully.");
    }

    public KVResponse listKeyspaces() {
        // list all keyspaces in the database
        QueryBuilder.QueryBuilder__21 queryBuilder = new QueryBuilder()
                .select()
                .column(KEYSPACE_NAME_COLUMN)
                .from(SYSTEM_SCHEMA, "keyspaces");
        QueryOuterClass.Response response;
        try {
            response = bridge.executeQuery(queryBuilder.build());
        } catch (Exception ex) {
            return new KVResponse(500, ex.getMessage());
        }
        List<String> allKeyspaceNames = response.getResultSet().getRowsList().stream()
                .map(row -> row.getValues(0))
                .map(QueryOuterClass.Value::getString)
                .collect(Collectors.toList());
        // set the message part as {"keyspaces": ["keyspace1", "keyspace2", ...]}
        return new KVResponse(200, "{\"keyspaces\": [" + String.join(", ", allKeyspaceNames) + "]}");
    }

    public KVResponse listTables(String keyspace_name) {
        // check if the keyspace exists
        QueryOuterClass.Query query_check = new QueryBuilder()
                .select()
                .column(KEYSPACE_NAME_COLUMN)
                .from(SYSTEM_SCHEMA, "keyspaces")
                .where(KEYSPACE_NAME_COLUMN,
                        Predicate.EQ,
                        QueryOuterClass.Value.newBuilder().setString(keyspace_name).build())
                .build();
        List<QueryOuterClass.Row> rows;
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
            // check if the keyspace exists
            rows = bridge.executeQuery(query_check).getResultSet().getRowsList();
            // if keyspaces exists, return 409
            if (rows.size() == 0) {
                return new KVResponse(409,
                        "The database '" + keyspace_name + "' not exists.");
            }
            response = bridge.executeQuery(queryBuilder.build());
        } catch (Exception ex) {
            return new KVResponse(500, ex.getMessage());
        }
        List<String> allTableNames = response.getResultSet().getRowsList().stream()
                .map(row -> row.getValues(0))
                .map(QueryOuterClass.Value::getString)
                .collect(Collectors.toList());
        // set the message part as {"tables": ["table1", "table2"]}
        return new KVResponse(200, "{\"tables\": [" + String.join(",", allTableNames) + "]}");
    }

    public boolean isKeyInTable(String keyspace_name, String table_name, String key) throws Exception {
        // check if the key exists in the table
        QueryOuterClass.Query query = new QueryBuilder()
                .select()
                .column("key")
                .from(keyspace_name, table_name)
                .where("key",
                        Predicate.EQ,
                        QueryOuterClass.Value.newBuilder().setString(key).build())
                .build();
        List<QueryOuterClass.Row> rows;
        rows = bridge.executeQuery(query).getResultSet().getRowsList();
        return rows.size() != 0;// true, row
    }

    public KVResponse putKeyVal(String keyspace_name, String table_name, String key, String value) {
        // insert the value into the table
        QueryOuterClass.Query query = new QueryBuilder()
                .insertInto(keyspace_name, table_name)
                .value("key", QueryOuterClass.Value.newBuilder().setString(key).build())
                .value("value", QueryOuterClass.Value.newBuilder().setString(value).build())
                .build();
        try {
            if (isKeyInTable(keyspace_name, table_name, key)) {
                return new KVResponse(409,
                        "The key '" + key + "' already exists.");
            }
            bridge.executeQuery(query);
        } catch (StatusRuntimeException ex) {
            // handle exception when Status is INVALID_ARGUMENT
            return new KVResponse(ex.getStatus().getCode().value(), ex.getMessage());
        } catch (Exception ex) {
            return new KVResponse(500, ex.getMessage());
        }
        return new KVResponse(201,
                "The key value pair '" + key + ":" + value + "' has been inserted successfully.");
    }

    public KVResponse getVal(String keyspace_name, String table_name, String key) {
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
            if (response.getResultSet().getRowsCount() == 0) {
                return new KVResponse(404,
                        "The key '" + key + "' not exists.");
            }
        } catch (StatusRuntimeException ex) {
            // handle exception when Status is INVALID_ARGUMENT
            return new KVResponse(ex.getStatus().getCode().value(), ex.getMessage());
        } catch (Exception ex) {
            return new KVResponse(500, ex.getMessage());
        }
        // get the row from the response
        QueryOuterClass.Row row = response.getResultSet().getRows(0);
        // get the value from the row
        String value = row.getValues(0).getString();
        return new KVResponse(200, value);
    }

    public KVResponse updateVal(String keyspace_name, String table_name, String key, String value) {
        // update the value in the table where key = key, if it exists, otherwise return
        QueryOuterClass.Query query = new QueryBuilder()
                .update(keyspace_name, table_name)
                .value("value", QueryOuterClass.Value.newBuilder().setString(value).build())
                .where("key", Predicate.EQ, QueryOuterClass.Value.newBuilder().setString(key).build())
                .build();
        try {
            if (!isKeyInTable(keyspace_name, table_name, key)) {
                return new KVResponse(404,
                        "The key '" + key + "' does not exist.");
            }
            bridge.executeQuery(query);
        } catch (StatusRuntimeException ex) {
            // handle exception when Status is INVALID_ARGUMENT
            return new KVResponse(ex.getStatus().getCode().value(), ex.getMessage());
        } catch (Exception ex) {
            return new KVResponse(500, ex.getMessage());
        }
        return new KVResponse(200,
                "The key value pair '" + key + ":" + value + "' has been updated successfully.");
    }

    public KVResponse deleteKey(String keyspace_name, String table_name, String key) {
        // delete the row from the table where key = key
        QueryOuterClass.Query query = new QueryBuilder()
                .delete()
                .from(keyspace_name, table_name)
                .where("key", Predicate.EQ, QueryOuterClass.Value.newBuilder().setString(key).build())
                .build();
        try {
            bridge.executeQuery(query);
        } catch (StatusRuntimeException ex) {
            // handle exception when Status is INVALID_ARGUMENT
            return new KVResponse(ex.getStatus().getCode().value(), ex.getMessage());
        } catch (Exception ex) {
            return new KVResponse(500, ex.getMessage());
        }
        return new KVResponse(200,
                "The key '" + key + "' has been deleted successfully.");
    }
}

