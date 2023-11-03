package org.stargate.rest.json;


public class KVResponse {
  // public Integer db_id;
  public int status_code = 200;
  public String message;

  public KVData body;

  public KVResponse() {}

  public KVResponse(int status_code, String message) {
    this.message = message;
    this.status_code = status_code;
  }

  public KVResponse(KVData body) {
    this.body = body;
  }
}
