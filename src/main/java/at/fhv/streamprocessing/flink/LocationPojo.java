package at.fhv.streamprocessing.flink;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.math.BigDecimal;

@JsonPropertyOrder({"country3","country2","country","region","subregion",
        "place_name","station_name","type", "stn_key", "status", "icao",
        "national_id", "wmo", "wban", "ghcn", "special", "lat", "lon", "elev", "tz"})
public class LocationPojo {
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
    public BigDecimal lat;
    public BigDecimal lng;
    public Double elev;
    public String tz;
}


