package org.example.ftp;

import org.apache.commons.net.ftp.FTPClient;
import java.io.InputStream;
import java.io.IOException;

public class FTPClientReader {
    private String server;
    private int port;
    private String user;
    private String pass;

    public FTPClientReader(String server, int port, String user, String pass) {
        this.server = server;
        this.port = port;
        this.user = user;
        this.pass = pass;
    }

    public InputStream readFile(String filePath) throws IOException {
        FTPClient ftpClient = new FTPClient();
        ftpClient.connect(server, port);
        ftpClient.login(user, pass);
        ftpClient.enterLocalPassiveMode();
        ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);

        InputStream inputStream = ftpClient.retrieveFileStream(filePath);
        if (inputStream == null) {
            throw new IOException("Unable to retrieve file from FTP server");
        }

        return inputStream;
    }
}
