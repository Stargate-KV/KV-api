package org.stargate.rest.json;

public class KvstoreException extends Exception {
	public Integer error_code;
	public String error_message;
	public KvstoreException() {}
	public KvstoreException(Integer error_code, String error_message) {
		this.error_code = error_code;
		this.error_message = error_message;
	}
}
