package org.stargate.rest.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.QueryOuterClass.Collection;
import io.stargate.bridge.proto.QueryOuterClass.Collection.Builder;
import io.stargate.bridge.proto.QueryOuterClass.Query;
import io.stargate.bridge.proto.QueryOuterClass.Value;
import io.stargate.bridge.proto.QueryOuterClass.Value.Null;
import io.stargate.sgv2.api.common.cql.builder.Column;
import io.stargate.sgv2.api.common.cql.builder.ImmutableColumn;
import io.stargate.sgv2.api.common.cql.builder.Predicate;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder.QueryBuilder__18;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder.QueryBuilder__26;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder.QueryBuilder__7;
import io.stargate.sgv2.api.common.cql.builder.Replication;
import io.stargate.sgv2.api.common.grpc.StargateBridgeClient;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class KVCassandra {
  private static final String TABLE_NAME = "table_name";
  private static final String SYSTEM_SCHEMA = "system_schema";
  private static final String TABLES = "tables";
  private static final String KEYSPACE_NAME_COLUMN = "keyspace_name";
  public static final Map<KVDataType, KVDataType> DATAMAP =
      Map.of(
          KVDataType.LISTINT, KVDataType.INT,
          KVDataType.LISTDOUBLE, KVDataType.DOUBLE,
          KVDataType.LISTTEXT, KVDataType.TEXT,
          KVDataType.SETINT, KVDataType.INT,
          KVDataType.SETDOUBLE, KVDataType.DOUBLE,
          KVDataType.SETTEXT, KVDataType.TEXT);
  @Inject StargateBridgeClient bridge;
  List<Column> columns = new ArrayList<>();

  public KVCassandra() {
    ImmutableColumn.Builder val_column = ImmutableColumn.builder().name("value_text").type("text");
    columns.add(val_column.build());

    columns.add(ImmutableColumn.builder().name("value_int").type("int").build());
    columns.add(ImmutableColumn.builder().name("value_double").type("double").build());

    columns.add(ImmutableColumn.builder().name("value_list_int").type("list<int>").build());
    columns.add(ImmutableColumn.builder().name("value_list_text").type("list<text>").build());
    columns.add(ImmutableColumn.builder().name("value_list_double").type("list<double>").build());

    columns.add(ImmutableColumn.builder().name("value_set_int").type("set<int>").build());
    columns.add(ImmutableColumn.builder().name("value_set_text").type("set<text>").build());
    columns.add(ImmutableColumn.builder().name("value_set_double").type("set<double>").build());
  }

  public KVResponse createKeyspace(String keyspace_name) {
    // create keyspace
    QueryOuterClass.Query query_create =
        new QueryBuilder()
            .create()
            .keyspace(keyspace_name)
            .withReplication(Replication.simpleStrategy(1))
            .build();

    try {
      bridge.executeQuery(query_create);
    } catch (StatusRuntimeException ex) {
      return handleStatusRuntimeException(ex, keyspace_name, null);
    } catch (Exception ex) {
      return new KVResponse(500, ex.getMessage());
    }
    return new KVResponse(
        201, "The database '" + keyspace_name + "' has been created successfully.");
  }

  public KVResponse deleteKeyspace(String keyspace_name) {
    QueryOuterClass.Query query = new QueryBuilder().drop().keyspace(keyspace_name).build();
    try {
      bridge.executeQuery(query);
    } catch (StatusRuntimeException ex) {
      return handleStatusRuntimeException(ex, keyspace_name, null);
    } catch (Exception ex) {
      return new KVResponse(500, ex.getMessage());
    }
    return new KVResponse(
        200, "The database '" + keyspace_name + "' has been deleted successfully.");
  }

  public KVResponse createTable(String keyspace_name, String table_name) {
    // build a partition key column and a value column
    ImmutableColumn.Builder key_column = ImmutableColumn.builder().name("key").type("text");
    key_column.kind(Column.Kind.PARTITION_KEY);

    QueryOuterClass.Query query =
        new QueryBuilder()
            .create()
            .table(keyspace_name, table_name)
            .column(columns)
            .column(key_column.build())
            .build();

    try {
      bridge.executeQuery(query);
    } catch (StatusRuntimeException ex) {
      return handleStatusRuntimeException(ex, keyspace_name, table_name);
    } catch (Exception ex) {
      return new KVResponse(500, ex.getMessage());
    }
    return new KVResponse(201, "The table '" + table_name + "' has been created successfully.");
  }

  public KVResponse deleteTable(String keyspace_name, String table_name) {
    QueryOuterClass.Query query =
        new QueryBuilder().drop().table(keyspace_name, table_name).build();
    try {
      bridge.executeQuery(query);
    } catch (StatusRuntimeException ex) {
      return handleStatusRuntimeException(ex, keyspace_name, table_name);
    } catch (Exception ex) {
      return new KVResponse(500, ex.getMessage());
    }
    return new KVResponse(200, "The table '" + table_name + "' has been deleted successfully.");
  }

  public KVResponse listKeyspaces() {
    // list all keyspaces in the database
    QueryBuilder.QueryBuilder__21 queryBuilder =
        new QueryBuilder().select().column(KEYSPACE_NAME_COLUMN).from(SYSTEM_SCHEMA, "keyspaces");
    QueryOuterClass.Response response;
    try {
      response = bridge.executeQuery(queryBuilder.build());
    } catch (StatusRuntimeException ex) {
      return handleStatusRuntimeException(ex, null, null);
    } catch (Exception ex) {
      return new KVResponse(500, ex.getMessage());
    }
    List<String> allKeyspaceNames =
        response.getResultSet().getRowsList().stream()
            .map(row -> row.getValues(0))
            .map(QueryOuterClass.Value::getString)
            .collect(Collectors.toList());
    // set the message part as {"keyspaces": ["keyspace1", "keyspace2", ...]}
    return new KVResponse(200, "{\"keyspaces\": [" + String.join(", ", allKeyspaceNames) + "]}");
  }

  public KVResponse listTables(String keyspace_name) {
    // check if the keyspace exists
    QueryOuterClass.Query query_check =
        new QueryBuilder()
            .select()
            .column(KEYSPACE_NAME_COLUMN)
            .from(SYSTEM_SCHEMA, "keyspaces")
            .where(
                KEYSPACE_NAME_COLUMN,
                Predicate.EQ,
                QueryOuterClass.Value.newBuilder().setString(keyspace_name).build())
            .build();
    List<QueryOuterClass.Row> rows;
    // list all tables in the keyspace
    QueryBuilder.QueryBuilder__21 queryBuilder =
        new QueryBuilder()
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
        return new KVResponse(404, "The database '" + keyspace_name + "' not exists.");
      }
      response = bridge.executeQuery(queryBuilder.build());
    } catch (StatusRuntimeException ex) {
      return handleStatusRuntimeException(ex, keyspace_name, null);
    } catch (Exception ex) {
      return new KVResponse(500, ex.getMessage());
    }
    List<String> allTableNames =
        response.getResultSet().getRowsList().stream()
            .map(row -> row.getValues(0))
            .map(QueryOuterClass.Value::getString)
            .collect(Collectors.toList());
    // set the message part as {"tables": ["table1", "table2"]}
    return new KVResponse(200, "{\"tables\": [" + String.join(",", allTableNames) + "]}");
  }

  public boolean isKeyInTable(String keyspace_name, String table_name, String key)
      throws Exception {
    // check if the key exists in the table
    QueryOuterClass.Query query =
        new QueryBuilder()
            .select()
            .column("key")
            .from(keyspace_name, table_name)
            .where("key", Predicate.EQ, QueryOuterClass.Value.newBuilder().setString(key).build())
            .build();
    List<QueryOuterClass.Row> rows;
    rows = bridge.executeQuery(query).getResultSet().getRowsList();
    return rows.size() != 0; // true, row
  }

  private Value getValue(JsonNode value, KVDataType type) {
    Value res = null;
    switch (type) {
      case INT:
        res = QueryOuterClass.Value.newBuilder().setInt(value.asLong()).build();
        break;
      case TEXT:
        res = QueryOuterClass.Value.newBuilder().setString(value.asText()).build();
        break;
      case DOUBLE:
        res = QueryOuterClass.Value.newBuilder().setDouble(value.asDouble()).build();
        break;
    }
    return res;
  }

  public KVResponse putKeyVal(
      String keyspace_name, String table_name, String key, JsonNode value, KVDataType type) {
    QueryBuilder__18 queryBuilder =
        new QueryBuilder()
            .insertInto(keyspace_name, table_name)
            .value("key", QueryOuterClass.Value.newBuilder().setString(key).build());
    QueryOuterClass.Query query = null;

    switch (type) {
      case INT:
      case TEXT:
      case DOUBLE:
        query = queryBuilder.value("value_" + type.label, getValue(value, type)).build();
        break;
      default:
        Builder collectionBuilder = QueryOuterClass.Value.newBuilder().getCollectionBuilder();

        for (JsonNode node : (ArrayNode) value) {
          collectionBuilder.addElements(getValue(node, DATAMAP.get(type)));
        }
        Collection collection = collectionBuilder.build();
        Value val = QueryOuterClass.Value.newBuilder().setCollection(collection).build();
        if (type.label.startsWith("list")) {
          query = queryBuilder.value("value_list_" + DATAMAP.get(type).label, val).build();
        } else {
          query = queryBuilder.value("value_set_" + DATAMAP.get(type).label, val).build();
        }
    }

    try {
      if (isKeyInTable(keyspace_name, table_name, key)) {
        return new KVResponse(409, "The key '" + key + "' already exists.");
      }
      bridge.executeQuery(query);
    } catch (StatusRuntimeException ex) {
      return handleStatusRuntimeException(ex, keyspace_name, table_name);
    } catch (Exception ex) {
      return new KVResponse(500, ex.getMessage());
    }
    return new KVResponse(
        201, "The key value pair '" + key + ":" + value + "' has been inserted successfully.");
  }

  public KVResponse getVal(String keyspace_name, String table_name, String key) {
    // select the value from the table where key = key
    QueryOuterClass.Query query =
        new QueryBuilder()
            .select()
            .column(columns)
            .from(keyspace_name, table_name)
            .where("key", Predicate.EQ, QueryOuterClass.Value.newBuilder().setString(key).build())
            .build();

    QueryOuterClass.Response response;

    try {
      response = bridge.executeQuery(query);
      if (response.getResultSet().getRowsCount() == 0) {
        return new KVResponse(
            404, "The key '" + key + "' cannot be found in the current database.");
      }
    } catch (StatusRuntimeException ex) {
      return handleStatusRuntimeException(ex, keyspace_name, table_name);
    } catch (Exception ex) {
      return new KVResponse(500, ex.getMessage());
    }
    // get the row from the response
    KVData body = new KVData();
    QueryOuterClass.Row row = response.getResultSet().getRows(0);
    Value value = null;
    for (int i = 0; i < columns.size(); i++) {
      if (!row.getValues(i).hasNull()) {
        body.type = KVDataType.get(columns.get(i).type());
        value = row.getValues(i);
        break;
      }
    }

    switch (body.type) {
      case INT:
        body.value_int = (int) (value.getInt());
        break;
      case DOUBLE:
        body.value_double = value.getDouble();
        break;
      case TEXT:
        body.value_text = value.getString();
        break;
      case LISTINT:
      case SETINT:
        List<Value> value_list = value.getCollection().getElementsList();
        body.list_int = new int[value_list.size()];
        for (int i = 0; i < value_list.size(); i++) {
          body.list_int[i] = (int) value_list.get(i).getInt();
        }
        break;
      case LISTDOUBLE:
      case SETDOUBLE:
        List<Value> value_list1 = value.getCollection().getElementsList();
        body.list_double = new double[value_list1.size()];
        for (int i = 0; i < value_list1.size(); i++) {
          body.list_double[i] = value_list1.get(i).getDouble();
        }
        break;
      case LISTTEXT:
      case SETTEXT:
        List<Value> value_list11 = value.getCollection().getElementsList();
        body.list_text = new String[value_list11.size()];
        for (int i = 0; i < value_list11.size(); i++) {
          body.list_text[i] = value_list11.get(i).getString();
        }
        break;
    }

    return new KVResponse(body);
  }

  public KVResponse updateVal(
      String keyspace_name, String table_name, String key, JsonNode value, KVDataType type) {
    // update the value in the table where key = key, if it exists, otherwise return

    QueryBuilder__7 queryBuilder = new QueryBuilder().update(keyspace_name, table_name);
    QueryBuilder__26 query;
    Null null_value = QueryOuterClass.Value.newBuilder().getNull();
    for (Column column : columns) {
      if (KVDataType.get(column.type()) != type) {
        query =
            queryBuilder.value(
                column, QueryOuterClass.Value.newBuilder().setNull(null_value).build());
      }
    }

    switch (type) {
      case INT:
      case TEXT:
      case DOUBLE:
        query = queryBuilder.value("value_" + type.label, getValue(value, type));
        break;
      default:
        Builder collectionBuilder = QueryOuterClass.Value.newBuilder().getCollectionBuilder();

        for (JsonNode node : (ArrayNode) value) {
          collectionBuilder.addElements(getValue(node, DATAMAP.get(type)));
        }
        Collection collection = collectionBuilder.build();
        Value val = QueryOuterClass.Value.newBuilder().setCollection(collection).build();
        if (type.label.startsWith("list")) {
          query = queryBuilder.value("value_list_" + DATAMAP.get(type).label, val);
        } else {
          query = queryBuilder.value("value_set_" + DATAMAP.get(type).label, val);
        }
    }

    Query final_query =
        query
            .where("key", Predicate.EQ, QueryOuterClass.Value.newBuilder().setString(key).build())
            .build();

    try {
      // if (!isKeyInTable(keyspace_name, table_name, key)) {
      //   return new KVResponse(
      //       404, "The key '" + key + "' cannot be found in the current database.");
      // }
      bridge.executeQuery(final_query);
    } catch (StatusRuntimeException ex) {
      return handleStatusRuntimeException(ex, keyspace_name, table_name);
    } catch (Exception ex) {
      return new KVResponse(500, ex.getMessage());
    }
    return new KVResponse(
        200, "The key value pair '" + key + ":" + value + "' has been updated successfully.");
  }

  public KVResponse deleteKey(String keyspace_name, String table_name, String key) {
    // delete the row from the table where key = key
    QueryOuterClass.Query query =
        new QueryBuilder()
            .delete()
            .from(keyspace_name, table_name)
            .where("key", Predicate.EQ, QueryOuterClass.Value.newBuilder().setString(key).build())
            .build();
    try {
      if (!isKeyInTable(keyspace_name, table_name, key)) {
        return new KVResponse(
            404, "The key '" + key + "' cannot be found in the current database.");
      }
      bridge.executeQuery(query);
    } catch (StatusRuntimeException ex) {
      return handleStatusRuntimeException(ex, keyspace_name, table_name);
    } catch (Exception ex) {
      return new KVResponse(500, ex.getMessage());
    }
    return new KVResponse(200, "The key '" + key + "' has been deleted successfully.");
  }

  private KVResponse handleStatusRuntimeException(
      StatusRuntimeException ex, String keyspace_name, String table_name) {
    System.out.println("got message:" + ex.getMessage());
    Status.Code code = ex.getStatus().getCode();
    String error_message = ex.getMessage();
    // System.out.println("Exception code: " + code.value());
    if (code == Status.Code.UNAUTHENTICATED) {
      return new KVResponse(
          401, "The request is unauthorized. Check if the X-Authentication-Token is correct.");
    } else if (code == Status.Code.INVALID_ARGUMENT) {
      String error_type =
          error_message.substring(error_message.indexOf(":") + 2, error_message.indexOf(":") + 3);
      if (error_type.equals("k") || error_type.equals("K")) {
        return new KVResponse(
            404, "The database with the database name '" + keyspace_name + "' could not be found.");
      } else {
        return new KVResponse(
            404, "The table with the table name '" + table_name + "' could not be found.");
      }
    } else if (code == Status.Code.ALREADY_EXISTS) {
      return new KVResponse(409, error_message.substring(error_message.indexOf(":") + 2));
    } else {
      return new KVResponse(
          503, "Service not available. Check if the server is running correctly.");
    }
  }
}
