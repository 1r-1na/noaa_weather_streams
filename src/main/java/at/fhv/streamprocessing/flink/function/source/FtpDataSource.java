package at.fhv.streamprocessing.flink.function.source;

import at.fhv.streamprocessing.flink.util.FileReadingFtpClient;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

public class FtpDataSource implements SourceFunction<String>, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(FtpDataSource.class);

    private final static int AMOUNT_OF_FILES_TO_MONITOR = 50;

    private final static String SERVER_IP = "ftp2.ncdc.noaa.gov";
    private final static int SERVER_PORT = 21;
    private final static String USERNAME = "anonymous";
    private final static String PASSWORD = "password";
    private final static String FILE_PATH = "/pub/data/noaa/2024";

    private boolean isCanceled = false;

    /**
     * filename, lastchange-millis, lastline
     */
    private Map<String, Tuple2<Long, Integer>> monitoredFiles;

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {

        FileReadingFtpClient ftpClient = FileReadingFtpClient.newInstance(SERVER_IP, SERVER_PORT, USERNAME, PASSWORD);
        ftpClient.changeDir(FILE_PATH);

        List<String> availableFiles = ftpClient.listFilesInCurrentFolder()
                .stream()
                .map(nameAndChangeDate -> nameAndChangeDate.f0)
                .filter(name -> !name.split("-")[1].equals("99999"))
                .collect(Collectors.toList());

        Collections.shuffle(availableFiles);

        monitoredFiles = availableFiles
                .subList(0, AMOUNT_OF_FILES_TO_MONITOR)
                .stream()
                .collect(Collectors.toMap(Function.identity(), (name) -> new Tuple2<>(0L, 0)));

        while (!isCanceled) {
            tick(sourceContext);
            Thread.sleep(1000 * 60 * 2);
        }

    }

    private void tick( SourceContext<String> sourceContext) throws IOException {

        LOG.info("Looking for changes in monitored files...");

        LinkedList<String> recordsToSend = new LinkedList<>();
        AtomicInteger fileDebugTracker = new AtomicInteger(0);

        FileReadingFtpClient ftpClient = FileReadingFtpClient.newInstance(SERVER_IP, SERVER_PORT, USERNAME, PASSWORD);
        ftpClient.changeDir(FILE_PATH);
        ftpClient.listFilesInCurrentFolder()
                .stream()
                .filter(nameAndTs -> monitoredFiles.containsKey(nameAndTs.f0))
                .filter(nameAndTs -> monitoredFiles.get(nameAndTs.f0).f0 < nameAndTs.f1)
                .forEach(nameAndTs -> {
                    if (isCanceled) {
                        return;
                    }
                    try {
                        String fileName = nameAndTs.f0;
                        long newLastChangeTs = nameAndTs.f1;
                        int previousReadLines = monitoredFiles.get(fileName).f1;

                        List<String> lines = ftpClient.readLinesOfFile(fileName);
                        List<String> unreadLines = lines.subList(previousReadLines, lines.size());
                        recordsToSend.addAll(unreadLines);

                        monitoredFiles.put(fileName, new Tuple2<>(newLastChangeTs, lines.size()));
                        LOG.info("Received {} new lines of file {} [{}/{}]", lines.size() - previousReadLines,  fileName, fileDebugTracker.incrementAndGet(), AMOUNT_OF_FILES_TO_MONITOR);

                    } catch (Exception e) {
                        LOG.error("Failed to load File {}", nameAndTs.f0, e);
                    }
                });

        Collections.shuffle(recordsToSend);

        LOG.info("[DONE] Looking for changes in monitored files");

        LOG.info("Sending new lines in ascending order");

        recordsToSend.sort(Comparator.comparing(e -> e.substring(15, 27)));
        recordsToSend.forEach(sourceContext::collect);

        LOG.info("[DONE] Sending new lines in ascending order");
    }

    @Override
    public void cancel() {
        isCanceled = true;
    }
}