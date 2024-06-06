package org.example.ftp;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

import java.io.*;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

public class FTPClientReader {
    private final FTPClient ftpClient;

    public FTPClientReader(FTPClient ftpClient) {
        this.ftpClient = ftpClient;
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

    public List<String> readLinesOfFilesInFolder(String folderPath) throws IOException {
        ftpClient.changeWorkingDirectory(folderPath);
        FTPFile[] remoteFiles = ftpClient.listFiles();
        return Arrays.stream(remoteFiles)
                .filter(FTPFile::isFile)
                .map(FTPFile::getName)
                .filter(name -> name.endsWith(".gz"))
                .flatMap(fileName -> {
                    System.out.println(fileName);
                    return readLinesOfFile(fileName).stream();
                })
                .collect(Collectors.toList());
    }

    public static FTPClientReader newInstance(String server, int port, String user, String pass) throws IOException {
        FTPClient ftpClient = new FTPClient();
        ftpClient.connect(server, port);
        ftpClient.login(user, pass);
        ftpClient.enterLocalPassiveMode();
        ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
        return new FTPClientReader(ftpClient);
    }
}
