package mmelsen;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Custom timestamp extractor to be able to deal with late-arriving-events. These are events that have
 * a timestamp that deviates from the actual timestamp in the message itself. In order to deal with late-
 * arriving-events the message will be translated by the {@link SensorMeasurementSerde} that will
 * create an object from where the timestamp can be extracted.
 */
@Slf4j
public class ErrorScoreTimestampExtractor implements TimestampExtractor {

    SensorMeasurementSerde serde = new SensorMeasurementSerde();

    @Override
    public long extract(final ConsumerRecord<Object, Object> record, final long previousTimestamp) {
        if (record != null && record.value() != null) {

            // use the custom deserializer to create the pojo
            SensorMeasurement sensorMeasurement = serde.deserializer().deserialize(String.valueOf(record.timestamp()), (byte[]) record.value());

            if(sensorMeasurement != null) {
                // use the timestamp from the payload
                return sensorMeasurement.getTs().getTime();
            }
        }
        log.warn("Record was not of type {} so we will fallback to using current time as the record key", SensorMeasurement.class.getName());
        // We will fallback to the currentTimeMillis as we prefer rather keeping the error-score in the resulting
        // dataset (albeit in possibly the wrong window) over discarding the error-score
        return System.currentTimeMillis();
    }
}
