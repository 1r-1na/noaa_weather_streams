package at.fhv.streamprocessing.flink.record;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({"country3","country2","country","region","subregion",
        "place_name","station_name","type", "stn_key", "status", "icao",
        "national_id", "wmo", "wban", "ghcn", "special", "lat", "lon", "elev", "tz"})
public class MlidRecord {
    public String country3;
    public String country2;
    public String country;
    public String region;
    public String subregion;
    public String place_name;
    public String station_name;
    public String type;
    public String stn_key;
    public String status;
    public String icao;
    public String national_id;
    public String wmo;
    public String wban;
    public String ghcn;
    public String special;
    public String lat;
    public String lon;
    public String elev;
    public String tz;


    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.JSON_STYLE);
    }
}

