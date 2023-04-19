package org.stargate.rest.json;

import com.fasterxml.jackson.annotation.JsonProperty;

public class KeyValPair {

  public String key;
  
  public KeyValPair() {}

  public KeyValPair(String key, String val) {
    this.key = key;
  }
}
