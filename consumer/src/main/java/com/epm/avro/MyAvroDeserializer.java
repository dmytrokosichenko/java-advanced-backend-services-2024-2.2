package com.epm.avro;

import jakarta.xml.bind.DatatypeConverter;
import lombok.extern.java.Log;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Arrays;

@Log
public class MyAvroDeserializer<T extends SpecificRecordBase> implements Deserializer<T> {

    protected final Class<T> targetType;

    public MyAvroDeserializer(Class<T> targetType) {
        this.targetType = targetType;
    }

    @SuppressWarnings("unchecked")
    public T deserialize(String topic, byte[] data) {
        try {
            T result = null;

            if (data != null) {
                log.info("data=" + DatatypeConverter.printHexBinary(data));

                DatumReader<GenericRecord> datumReader =
                        new SpecificDatumReader<>(targetType.newInstance().getSchema());
                Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);

                result = (T) datumReader.read(null, decoder);
                log.info("deserialized data=" + result);
            }
            return result;
        } catch (Exception ex) {
            throw new SerializationException(
                    "Deserializition error. Data '" + Arrays.toString(data) + "' from topic '" + topic + "'", ex);
        }
    }
}
