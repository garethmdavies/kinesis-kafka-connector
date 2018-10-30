package com.amazon.kinesis.kafka;

import org.apache.kafka.connect.data.Schema;

import java.nio.ByteBuffer;

public interface KafkaRecordParser {

  ByteBuffer parseValue(Schema schema, Object value);

}
