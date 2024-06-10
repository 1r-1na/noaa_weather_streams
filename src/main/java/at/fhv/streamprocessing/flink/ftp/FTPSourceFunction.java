package at.fhv.streamprocessing.flink.ftp;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

public class FTPSourceFunction implements SourceFunction<String>, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(FTPSourceFunction.class);
    
    
    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        
        String server = "ftp2.ncdc.noaa.gov";
        int port = 21;
        String user = "anonymous";
        String pass = "password";
        String filePath = "/pub/data/noaa/2022";
        
        FTPClientReader ftpClient = FTPClientReader.newInstance(server, port, user, pass);        
        ftpClient.changeDir(filePath);
        List<String> fileNames = ftpClient.listFilesInCurrentFolder();
        for (String fileName : fileNames) {
            try {
                ftpClient.readLinesOfFile(fileName).forEach(line -> {
                    sourceContext.collect(line);
                });
            } catch (Exception e) {
                LOG.error("Failed to load File {}", fileName, e);
            }
        }
        
    }

    @Override
    public void cancel() {
        // not needed
    }
}