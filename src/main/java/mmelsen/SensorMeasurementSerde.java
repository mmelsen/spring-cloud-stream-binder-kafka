package mmelsen;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Custom deserializer that will deserialize from a byte[] to {@link SensorMeasurement}
 */
public class SensorMeasurementSerde implements Serde<SensorMeasurement> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {}

    @Override
    public void close() {}

    @Override
    public Serializer<SensorMeasurement> serializer() {
        return new Serializer<SensorMeasurement>() {

            @Override
            public void configure(Map<String, ?> map, boolean b) {
            }

            @Override
            public byte[] serialize(String arg0, SensorMeasurement sensorMeasurement) {
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
    public Deserializer<SensorMeasurement> deserializer() {
        return new Deserializer<SensorMeasurement>() {
            @Override
            public void configure(Map<String, ?> map, boolean b) {

            }

            @Override
            public SensorMeasurement deserialize(String s, byte[] bytes) {
                ObjectMapper mapper = new ObjectMapper();
                SensorMeasurement measurement = null;
                try {
                    measurement = mapper.readValue(bytes, SensorMeasurement.class);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return measurement;
            }

            @Override
            public void close() {
            }
        };
    }
}
