package at.fhv.streamprocessing.flink.record;

public class LiquidPrecipitation {
    private final int measuredTimeInHours;
    private final double depthInMillimeters;
    private final String qualityCode;
    private final boolean isValid;

    public LiquidPrecipitation(int measuredTimeInHours, double depthInMillimeters, String qualityCode) {
        this.measuredTimeInHours = measuredTimeInHours;
        this.depthInMillimeters = depthInMillimeters;
        this.qualityCode = qualityCode;
        this.isValid = true;
    }

    public LiquidPrecipitation(String qualityCode) {
        this.measuredTimeInHours = 99;
        this.depthInMillimeters = 9999;
        this.qualityCode = qualityCode;
        this.isValid = false;
    }

    public int measuredTimeInHours() {
        return measuredTimeInHours;
    }

    public double depthInMillimeters() {
        return depthInMillimeters;
    }

    public String qualityCode() {
        return qualityCode;
    }

    public boolean isValid() {
        return isValid;
    }
}
