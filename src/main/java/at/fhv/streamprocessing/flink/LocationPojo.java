package at.fhv.streamprocessing.flink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.util.function.SerializableFunction;

import java.io.File;
import java.math.BigDecimal;
import java.util.function.Function;

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
    public String lat;
    public String lon;
    public String elev;
    public String tz;

    @Override
    public String toString() {
        return "LocationPojo{" +
                "country3='" + country3 + '\'' +
                ", country2='" + country2 + '\'' +
                ", country='" + country + '\'' +
                ", region='" + region + '\'' +
                ", subregion='" + subregion + '\'' +
                ", place_name='" + place_name + '\'' +
                ", station_name='" + station_name + '\'' +
                ", type='" + type + '\'' +
                ", stn_key='" + stn_key + '\'' +
                ", status='" + status + '\'' +
                ", icao='" + icao + '\'' +
                ", national_id='" + national_id + '\'' +
                ", wmo='" + wmo + '\'' +
                ", wban='" + wban + '\'' +
                ", ghcn='" + ghcn + '\'' +
                ", special='" + special + '\'' +
                ", lat='" + lat + '\'' +
                ", lon='" + lon + '\'' +
                ", elev='" + elev + '\'' +
                ", tz='" + tz + '\'' +
                '}';
    }
}

