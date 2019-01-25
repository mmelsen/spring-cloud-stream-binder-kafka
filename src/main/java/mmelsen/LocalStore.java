package mmelsen;

import java.util.concurrent.TimeUnit;

public enum LocalStore {

    ONE_HOUR_STORE(0, "one-hour-error-score", TimeUnit.HOURS.toMillis(1)),
    SIX_HOUR_STORE(1, "six-hours-error-score", TimeUnit.HOURS.toMillis(6)),
    TWELVE_HOURS_STORE(2, "twelve-hours-error-score", TimeUnit.HOURS.toMillis(12)),
    ONE_DAY_STORE(3, "one-day-error-score", TimeUnit.DAYS.toMillis(1)),
    ONE_WEEK_STORE(4, "one-week-error-score", TimeUnit.DAYS.toMillis(7));

    private final long windowSizeInMillis;
    private final String storeName;
    private final int resolution;

    LocalStore(int resolution, String storeName, long windowSizeInMillis) {
        this.resolution = resolution;
        this.storeName = storeName;
        this.windowSizeInMillis = windowSizeInMillis;
    }

    public static LocalStore byResolution(int resolution) {
        switch (resolution) {
            case 0:
                return ONE_HOUR_STORE;
            case 1:
                return SIX_HOUR_STORE;
            case 2:
                return TWELVE_HOURS_STORE;
            case 3:
                return ONE_DAY_STORE;
            case 4:
                return ONE_WEEK_STORE;
            default:
                throw new IllegalStateException("Resolution: " + resolution + " is an unknown WindowStore type");
        }
    }

    public long getTimeUnit() {
        return windowSizeInMillis;
    }

    public String getStoreName() {
        return storeName;
    }
}
