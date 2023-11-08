package org.stargate.rest.json;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(using = KVSerializer.class)
public class KVData {

  public Integer value_int;

  public Double value_double;

  public String value_text;

  public int[] list_int;

  public double[] list_double;

  public String[] list_text;

  @JsonIgnore public KVDataType type;

  public KVData() {}

  public KVData(KVDataType type) {
    this.type = type;
  }

  public JsonNode getJsonBody() {
    ObjectMapper mapper = new ObjectMapper();

    switch (type) {
      case INT:
        return mapper.valueToTree(value_int);
      case DOUBLE:
        return mapper.valueToTree(value_double);
      case TEXT:
        return mapper.valueToTree(value_text);
      case LISTINT:
      case SETINT:
        return mapper.valueToTree(list_int);
      case LISTDOUBLE:
      case SETDOUBLE:
        return mapper.valueToTree(list_double);
      case LISTTEXT:
      case SETTEXT:
        return mapper.valueToTree(list_text);
      default:
        return mapper.createObjectNode();
    }
  }
}
