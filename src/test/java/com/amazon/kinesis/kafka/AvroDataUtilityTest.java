package com.amazon.kinesis.kafka;

import static org.testng.Assert.assertEquals;


import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.testng.annotations.Test;

public class AvroDataUtilityTest {

  @Test
  void parseAvroMessageWithSchema() throws Exception {

    Schema schema = SchemaBuilder.struct()
                                    .name("order")
                                    .field("item", Schema.STRING_SCHEMA)
                                    .field("price", Schema.FLOAT32_SCHEMA)
                                    .field("qty", Schema.INT8_SCHEMA)
                                    .build();

    Struct struct = new Struct(schema)
        .put("item", "Blue Box")
        .put("price",2.38f)
        .put("qty", (byte) 2);

    ByteBuffer actual = new AvroDataUtility(50).parseValue(schema, struct);

    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
    GenericRecord instance;
    try (
        SeekableByteArrayInput sbai = new SeekableByteArrayInput(actual.array());
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(sbai, datumReader);
    ) {
      instance = dataFileReader.next();

      assertEquals("Blue Box", instance.get("item").toString());
      assertEquals("2.38", instance.get("price").toString());
      assertEquals("2", instance.get("qty").toString());
    } catch (IOException ioe) {
      throw new Exception("Failed to deserialize Avro", ioe);
    }
  }

}
