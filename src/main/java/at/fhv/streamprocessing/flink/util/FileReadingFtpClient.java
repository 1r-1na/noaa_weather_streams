package at.fhv.streamprocessing.flink.util;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

import java.io.*;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

public class FileReadingFtpClient {
    private final FTPClient ftpClient;

    public FileReadingFtpClient(FTPClient ftpClient) {
        this.ftpClient = ftpClient;
    }

    public void changeDir(String folderPath) {
        try {
            ftpClient.changeWorkingDirectory(folderPath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public List<String> listFilesInCurrentFolder() {
        try {
            return Arrays.stream(ftpClient.listFiles())
                .filter(FTPFile::isFile)
                .map(FTPFile::getName)
                .filter(name -> name.endsWith(".gz"))
                .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public InputStream readFile(String filePath) throws IOException {
        InputStream inputStream = ftpClient.retrieveFileStream(filePath);
        if (inputStream == null) {
            throw new IOException("Unable to retrieve file from FTP server");
        }
        ftpClient.completePendingCommand();
        return inputStream;
    }

    public List<String> readLinesOfFile(String filePath) {

        try(InputStream stream = readFile(filePath);
            GZIPInputStream gzipInputStream = new GZIPInputStream(stream);
            BufferedReader reader = new BufferedReader(new InputStreamReader(gzipInputStream))) {
            String line = reader.readLine();
            List<String> appender = new LinkedList<>();
            while (line != null) {
                appender.add(line);
                line = reader.readLine();
            }
            return appender;

        } catch (Exception e) {
            System.out.println("File " + filePath + " seems to be corrupted!");
            e.printStackTrace();
            return List.of();
        }

    }

    public static FileReadingFtpClient newInstance(String server, int port, String user, String pass) throws IOException {
        FTPClient ftpClient = new FTPClient();
        ftpClient.connect(server, port);
        ftpClient.login(user, pass);
        ftpClient.enterLocalPassiveMode();
        ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
        return new FileReadingFtpClient(ftpClient);
    }
}
