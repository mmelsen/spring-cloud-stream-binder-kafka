package mmelsen.errorscore.raw.boundary;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;


@Slf4j
@EnableBinding(SensorMeasurementBinding.class)
public class Consumer {

    @StreamListener(SensorMeasurementBinding.ERROR_SCORE_IN)
    public void process(KStream<Object, String> input) {

        Materialized<String, String, WindowStore<Bytes, byte[]>> store = Materialized.as("store");
        input
                .map((k,v) -> new KeyValue<>(v, v))
                .groupByKey()
                .windowedBy(TimeWindows.of(5000))
                .reduce((aggValue, newValue) -> {
                    System.out.println("aggValue: " + aggValue);
                    System.out.println("newValue: " + newValue);
                    if(aggValue.endsWith("1")) {
                        return newValue;
                    }
                    else {
                        System.out.println("keeping aggValue: " + aggValue );
                        return aggValue;
                    }

                }, (store));


    }

}

