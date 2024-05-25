package org.example.ftp.fileSources;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.io.*;
import java.util.zip.GZIPInputStream;

public class FTPFileSource implements SourceFunction<String> {

    private final String ftpHost;
    private final int ftpPort;
    private final String ftpUsername;
    private final String ftpPassword;
    private final String filePath;

    public FTPFileSource(String ftpHost, int ftpPort, String ftpUsername, String ftpPassword, String filePath) {
        this.ftpHost = ftpHost;
        this.ftpPort = ftpPort;
        this.ftpUsername = ftpUsername;
        this.ftpPassword = ftpPassword;
        this.filePath = filePath;
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws IOException {
        // FTP server credentials
        String server = ftpHost;
        int port = 21;
        String username = ftpUsername;
        String password = ftpPassword;

        // Directory for storing downloaded files
        String downloadDir = filePath;

        // Connect to the FTP server
        FTPClient ftpClient = new FTPClient();
        try {
            ftpClient.connect(server, port);
            ftpClient.login(username, password);
            ftpClient.enterLocalPassiveMode();

            // Set binary file transfer mode
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

            // Change to the desired directory on the FTP server
            ftpClient.changeWorkingDirectory(downloadDir);

            System.out.println("Reading files...");
            // Get a list of all files in the directory
            FTPFile[] files = ftpClient.listFiles();


            // Iterate over the files and download them
            for (FTPFile file : files) {
                if (file.isFile()) {
                    String fileName = file.getName();

                    // Download the file
                    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                        ftpClient.retrieveFile(fileName, outputStream);

                        // Extract the file contents to a string
                        String fileContents = extractFileContents(outputStream.toByteArray());
                        for (String line : fileContents.split("\n"))
                        {
                            sourceContext.collect(line);
                        }
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // Disconnect from the FTP server
            if (ftpClient.isConnected()) {
                try {
                    ftpClient.logout();
                    ftpClient.disconnect();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static String extractFileContents(byte[] gzData) {
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(gzData);
             GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream);
             BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(gzipInputStream))) {

            StringBuilder stringBuilder = new StringBuilder();
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                stringBuilder.append(line).append(System.lineSeparator());
            }

            return stringBuilder.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }



    @Override
    public void cancel() {
        // No cleanup necessary
    }
}