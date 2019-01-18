package mmelsen.errorscore.raw.boundary;

import lombok.extern.slf4j.Slf4j;
import mmelsen.errorscore.raw.entity.ErrorScore;
import mmelsen.errorscore.raw.entity.SensorMeasurement;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;


@Slf4j
@EnableBinding(SensorMeasurementBinding.class)
public class Consumer {

    @StreamListener(SensorMeasurementBinding.ERROR_SCORE_IN)
    public void process(KStream<String, SensorMeasurement> events) {


        Materialized<String, ErrorScore, WindowStore<Bytes, byte[]>> oneHour = Materialized.as("store");
        events
                .map(getStringSensorMeasurementKeyValueKeyValueMapper())
                .groupByKey()
                .windowedBy(TimeWindows.of(2000))
                .reduce((aggValue, newValue) -> getMaxErrorScore(aggValue, newValue),
                        (oneHour));


    }

    private KeyValueMapper<String, SensorMeasurement, KeyValue<? extends String, ? extends ErrorScore>> getStringSensorMeasurementKeyValueKeyValueMapper() {
        return (s, sensorMeasurement) -> new KeyValue<>(s + "::" + sensorMeasurement.getT(),
                new ErrorScore(sensorMeasurement.getTs(), sensorMeasurement.getE()));
    }

    /**
     * Determine the max error score to keep by looking at the aggregated error signal and
     * freshly consumed error signal
     *
     * @param aggValue
     * @param newValue
     * @return
     */
    private ErrorScore getMaxErrorScore(ErrorScore aggValue, ErrorScore newValue) {
        if(aggValue.getErrorSignal() > newValue.getErrorSignal()) {
            return aggValue;
        }
        else {
            return newValue;
        }
    }
}

