package mmelsen.errorscore.raw.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Date;

/**
 * Pojo to hold only the timestamp and the error signal
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class ErrorScore {
    /**
     * indicates when the value was measured
     */
    private Date timestamp;

    /**
     * contains the actual error signal
     */
    private double errorSignal;
}
