package mmelsen.errorscore.raw.boundary;

import org.apache.kafka.streams.kstream.KStream;
import org.springframework.cloud.stream.annotation.Input;


public interface SensorMeasurementBinding {

    String ERROR_SCORE_IN = "error-score-in";
//    String ERROR_SCORE_OUT = "error-score-out";

    @Input(ERROR_SCORE_IN)
    KStream<String, ?> errorScoreIn();
//
//    @Output(ERROR_SCORE_OUT)
//    MessageChannel errorScoreOut();
}