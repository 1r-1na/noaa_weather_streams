package org.example;

import at.fhv.streamprocessing.flink.ftp.FTPClientReader;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class FTPFlink extends KeyedProcessFunction<Long, Transaction, Alert> {
    private final String filePath;
    private FTPClientReader ftpClientReader;

    public FTPFlink(String server, int port, String user, String pass, String filePath) throws IOException {
        this.filePath = filePath;

        ftpClientReader = FTPClientReader.newInstance(server, port, user, pass);
    }


    @Override
    public void processElement(
            Transaction transaction,
            Context context,
            Collector<Alert> collector) throws Exception {


        try (InputStream inputStream = ftpClientReader.readFile(filePath);
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {

            String line = reader.readLine();
            if (line != null) {
                Alert alert = new Alert();

                System.out.println(line);

                alert.setId(transaction.getAccountId());

                collector.collect(alert);
            }
        }
    }
}
