package at.fhv.streamprocessing.flink.ftp;

import java.io.IOException;
import java.util.List;

public class FTPTester {


    public static void main(String[] args) throws IOException {

        String server = "ftp2.ncdc.noaa.gov";
        int port = 21;
        String user = "anonymous";
        String pass = "password";
        String filePath = "/pub/data/noaa/1901";
        FTPClientReader client = FTPClientReader.newInstance(server, port, user, pass);

        List<String> lines = client.readLinesOfFilesInFolder(filePath);
        lines.forEach(System.out::println);


    }

}
