package org.stargate.rest.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;

public class KVSerializer extends StdSerializer<KVData> {

  public KVSerializer() {
    this(null);
  }

  public KVSerializer(Class<KVData> t) {
    super(t);
  }

  @Override
  public void serialize(KVData value, JsonGenerator jgen, SerializerProvider provider)
      throws IOException, JsonProcessingException {

    jgen.writeStartObject();
    jgen.writeStringField("type", value.type.label);
    switch (value.type) {
      case INT:
        jgen.writeNumberField("value", value.value_int);
        break;
      case DOUBLE:
        jgen.writeNumberField("value", value.value_double);
        break;
      case TEXT:
        jgen.writeStringField("value", value.value_text);
        break;
      case LISTINT:
      case SETINT:
        jgen.writeArrayFieldStart("value");
        jgen.writeArray(value.list_int, 0, value.list_int.length);
        jgen.writeEndArray();
        break;
      case LISTDOUBLE:
      case SETDOUBLE:
        jgen.writeArrayFieldStart("value");
        jgen.writeArray(value.list_double, 0, value.list_double.length);
        jgen.writeEndArray();
        break;

      case LISTTEXT:
      case SETTEXT:
        jgen.writeArrayFieldStart("value");
        jgen.writeArray(value.list_text, 0, value.list_text.length);
        jgen.writeEndArray();
        break;
    }
    jgen.writeEndObject();
  }
}
