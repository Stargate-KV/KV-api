package org.stargate.rest.json;

import java.util.HashMap;
import java.util.Map;

public enum KVDataType {
  INT("int"),
  DOUBLE("double"),
  TEXT("text"),
  LISTINT("list<int>"),
  LISTDOUBLE("list<double>"),
  LISTTEXT("list<text>"),
  SETINT("set<int>"),
  SETDOUBLE("set<double>"),
  SETTEXT("set<text>");
  public final String label;
  private static Map<String, KVDataType> lookup = new HashMap<String, KVDataType>();

  static {
    for (KVDataType d : KVDataType.values()) {
      lookup.put(d.label, d);
    }
  }

  private KVDataType(String label) {
    this.label = label;
  }

  public static KVDataType get(String label) {
    return lookup.get(label);
  }
}
