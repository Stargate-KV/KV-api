package org.stargate.rest.json;

public class db_response {
  public Integer db_id;

  public db_response() {}

  public db_response(int db_id) {
    this.db_id = Integer.valueOf(db_id);
  }
}
