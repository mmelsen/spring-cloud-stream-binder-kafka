package mmelsen.errorscore.raw.boundary;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.utils.Bytes;
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


        Materialized<Object, String, WindowStore<Bytes, byte[]>> store = Materialized.as("store");
        input
                .groupByKey()
                .windowedBy(TimeWindows.of(10000))
                .reduce((aggValue, newValue) -> aggValue.concat(" ").concat(newValue), (store));

    }

}

