package org.stargate.rest.json;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(using = KVSerializer.class)
public class KVData {

	  public Integer value_int;

	  public Double value_double;

	  public String value_text;
	 
	  public int[] list_int;

	  public double[] list_double;

	  public String[] list_text;
	  
	  @JsonIgnore
	  public KVDataType type;
	  

	  public KVData() {}
	  public KVData(KVDataType type) {
		  this.type = type;
	  }
	  
}
