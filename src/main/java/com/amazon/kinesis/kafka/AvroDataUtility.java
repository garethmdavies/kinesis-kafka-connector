package com.amazon.kinesis.kafka;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import io.confluent.connect.avro.AvroData;

public class AvroDataUtility implements KafkaRecordParser {

	private Integer DEFAULT_SCHEMA_SIZE = 50;
	private AvroData avroDataHelper;

	public AvroDataUtility(Integer schemaCacheSize) {
		this.avroDataHelper = new AvroData(schemaCacheSize != null ? schemaCacheSize : DEFAULT_SCHEMA_SIZE);
	}

	/**
	 * Parses Kafka Avro Values
	 *
	 * @param schema
	 *            - Schema of passed message as per
	 *            https://kafka.apache.org/0100/javadoc/org/apache/kafka/connect/data/Schema.html
	 * @param value
	 *            - Value of the message
	 * @return Parsed bytebuffer as per schema type
	 */
	public ByteBuffer parseValue(Schema schema, Object value) {

		DatumWriter<GenericRecord> datumWriter;
		datumWriter = new GenericDatumWriter<>();

		GenericRecord avroInstance = (GenericRecord)avroDataHelper.fromConnectData(schema, value);

		try (
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
		) {
			dataFileWriter.setCodec(CodecFactory.nullCodec());


			dataFileWriter.create(avroInstance.getSchema(), baos);

			dataFileWriter.append(avroInstance);
			dataFileWriter.flush();

			return ByteBuffer.wrap(baos.toByteArray());

		} catch (IOException ioe) {
			throw new DataException("Error serializing Avro", ioe);
		}
	}

}
