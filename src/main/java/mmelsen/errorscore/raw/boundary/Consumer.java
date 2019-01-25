package mmelsen.errorscore.raw.boundary;

import lombok.extern.slf4j.Slf4j;
import mmelsen.ErrorScore;
import mmelsen.ErrorScoreSerde;
import mmelsen.LocalStore;
import mmelsen.SensorMeasurement;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;

import java.util.Calendar;


@Slf4j
@EnableBinding(SensorMeasurementBinding.class)
public class Consumer {

    @StreamListener(SensorMeasurementBinding.ERROR_SCORE_IN)
    public void process(KStream<String, SensorMeasurement> events) {

        Materialized<String, ErrorScore, WindowStore<Bytes, byte[]>> store = Materialized.as("store");
        events
                .map(getStringSensorMeasurementKeyValueKeyValueMapper())
                .groupByKey()
                .windowedBy(TimeWindows.of(5000))
                /*.reduce((aggValue, newValue) -> {
                    System.out.println("aggValue: " + aggValue);
                    System.out.println("newValue: " + newValue);
                    if(aggValue.endsWith("1")) {
                        return "testertje";
                    }
                    else {
                        System.out.println("keeping aggValue: " + aggValue );
                        return aggValue;
                    }

                }, (store));*/
                .reduce((aggValue, newValue) -> getMaxErrorScore(aggValue, newValue), store);


        LocalStore oneHourStore = LocalStore.ONE_HOUR_STORE;
        LocalStore sixHourStore = LocalStore.SIX_HOUR_STORE;
        LocalStore twelveHoursStore = LocalStore.TWELVE_HOURS_STORE;
        LocalStore oneDayStore = LocalStore.ONE_DAY_STORE;
        LocalStore oneWeekStore = LocalStore.ONE_WEEK_STORE;

        // One hour store
        Materialized<String, ErrorScore, WindowStore<Bytes, byte[]>> oneHour = Materialized.as(oneHourStore.getStoreName());
        events
                .map(getStringSensorMeasurementKeyValueKeyValueMapper())
                .groupByKey()
                .windowedBy(TimeWindows.of(oneHourStore.getTimeUnit()))
                .reduce((aggValue, newValue) -> getMaxErrorScore(aggValue, newValue),
                        (oneHour));

        // Six hours store
        Materialized<String, ErrorScore, WindowStore<Bytes, byte[]>> sixHour = Materialized.as(sixHourStore.getStoreName());
        events
                .map(getStringSensorMeasurementKeyValueKeyValueMapper())
                .groupByKey()
                .windowedBy(TimeWindows.of(sixHourStore.getTimeUnit()))
                .reduce((aggValue, newValue) -> getMaxErrorScore(aggValue, newValue),
                        (sixHour.withValueSerde(new ErrorScoreSerde())));

        // Twelve hours store
        Materialized<String, ErrorScore, WindowStore<Bytes, byte[]>> twelveHours = Materialized.as(twelveHoursStore.getStoreName());
        events
                .map(getStringSensorMeasurementKeyValueKeyValueMapper())
                .groupByKey()
                .windowedBy(TimeWindows.of(twelveHoursStore.getTimeUnit()))
                .reduce((aggValue, newValue) -> getMaxErrorScore(aggValue, newValue),
                        (twelveHours.withValueSerde(new ErrorScoreSerde())));

        // One day store
        Materialized<String, ErrorScore, WindowStore<Bytes, byte[]>> oneDay = Materialized.as(oneDayStore.getStoreName());
        events
                .map(getStringSensorMeasurementKeyValueKeyValueMapper())
                .groupByKey()
                .windowedBy(TimeWindows.of(oneDayStore.getTimeUnit()))
                .reduce((aggValue, newValue) -> getMaxErrorScore(aggValue, newValue),
                        (oneDay.withValueSerde(new ErrorScoreSerde())));

        // One week store
        Materialized<String, ErrorScore, WindowStore<Bytes, byte[]>> oneWeek = Materialized.as(oneWeekStore.getStoreName());
        events
                .map(getStringSensorMeasurementKeyValueKeyValueMapper())
                .groupByKey()
                .windowedBy(TimeWindows.of(oneWeekStore.getTimeUnit()))
                .reduce((aggValue, newValue) -> getMaxErrorScore(aggValue, newValue),
                        (oneWeek.withValueSerde(new ErrorScoreSerde())));


    }
    private KeyValueMapper<String, SensorMeasurement, KeyValue<? extends String, ? extends ErrorScore>> getStringSensorMeasurementKeyValueKeyValueMapper() {
        return (s, sensorMeasurement) -> new KeyValue<>(s + "::" + sensorMeasurement.getT(),
                new ErrorScore(sensorMeasurement.getTs(), sensorMeasurement.getE()));
    }

    private ErrorScore getMaxErrorScore(ErrorScore aggValue, ErrorScore newValue) {
        if(aggValue.getErrorSignal() > newValue.getErrorSignal()) {
            return aggValue;
        }
        else {
            return newValue;
        }
    }
}


