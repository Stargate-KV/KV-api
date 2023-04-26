package org.stargate.rest.json;

public class KVResponse {
  // public Integer db_id;
  public int status_code;
  public String message;

  public KVResponse() {}

  public KVResponse(int status_code, String message) {
    this.message = message;
    this.status_code = status_code;
  }
}
