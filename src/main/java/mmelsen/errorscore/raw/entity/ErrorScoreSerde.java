package mmelsen.errorscore.raw.entity;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Serializer and Deserializer that can be used to serialize and deserialize
 * between byte arrays and {@link ErrorScore} objects.
 */
public class ErrorScoreSerde implements Serde<ErrorScore> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }

    @Override
    public Serializer<ErrorScore> serializer() {
        return new Serializer<ErrorScore>() {

            @Override
            public void configure(Map<String, ?> map, boolean b) {
            }

            @Override
            public byte[] serialize(String arg0, ErrorScore sensorMeasurement) {
                byte[] retVal = null;
                ObjectMapper objectMapper = new ObjectMapper();
                try {
                    retVal = objectMapper.writeValueAsString(sensorMeasurement).getBytes();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return retVal;
            }

            public void close() {
            }
        };
    }

    @Override
    public Deserializer<ErrorScore> deserializer() {
        return new Deserializer<ErrorScore>() {
            @Override
            public void configure(Map<String, ?> map, boolean b) {

            }

            @Override
            public ErrorScore deserialize(String s, byte[] bytes) {
                ObjectMapper mapper = new ObjectMapper();
                ErrorScore errorScore = null;
                try {
                    errorScore = mapper.readValue(bytes, ErrorScore.class);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return errorScore;
            }

            @Override
            public void close() {
            }
        };
    }
}