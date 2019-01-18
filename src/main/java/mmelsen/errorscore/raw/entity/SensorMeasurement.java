package mmelsen.errorscore.raw.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;
import java.util.Date;

/**
 * POJO to hold the aggregated data:
 * - Timestamp
 * - Tag
 * - Original value
 * - Reconstructed value
 * - Error score
 * - Scaler
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class SensorMeasurement implements Serializable {

    /**
     * indicates when the value was measured
     */
    private Date ts;

    /**
     * the identification of the sensor
     */
    private String t;

    /**
     * original measured value of the sensor
     */
    private double o;

    /**
     * value after running through neural network
     */
    private double r;

    /**
     * the error score calculated based on original and reconstructed
     */
    private double e;
}
