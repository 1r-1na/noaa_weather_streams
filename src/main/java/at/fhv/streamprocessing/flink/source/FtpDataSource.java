package at.fhv.streamprocessing.flink.source;

import at.fhv.streamprocessing.flink.util.FileReadingFtpClient;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

public class FtpDataSource implements SourceFunction<String>, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(FtpDataSource.class);

    private boolean isCanceled = false;
    
    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        String server = "ftp2.ncdc.noaa.gov";
        int port = 21;
        String user = "anonymous";
        String pass = "password";
        String filePath = "/pub/data/noaa/2022";
        
        FileReadingFtpClient ftpClient = FileReadingFtpClient.newInstance(server, port, user, pass);
        ftpClient.changeDir(filePath);
        List<String> fileNames = ftpClient.listFilesInCurrentFolder();
        for (String fileName : fileNames) {
            LOG.info(fileName);
            if (isCanceled) {
                return;
            }
            if(fileName.split("-")[1].equals("99999")) {
                continue;
            }
            try {
                ftpClient.readLinesOfFile(fileName).forEach(sourceContext::collect);
            } catch (Exception e) {
                LOG.error("Failed to load File {}", fileName, e);
            }
        }
        
    }

    @Override
    public void cancel() {
        isCanceled = true;
    }
}