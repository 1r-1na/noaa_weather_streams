package at.fhv.streamprocessing.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DemoWeatherDataJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> transactions = env
                .addSource(new WeatherDataMockSource())
                .name("weather-data");

        DataStream<Double> temperatures = transactions
                .keyBy(a -> a)
                .process(new TemperatureExtractor())
                .name("temperatures");

        temperatures
                .addSink(new DemoDoubleLoggingSink())
                .name("logging-sink");

        env.execute("weather-data-demo-job");
    }
}
