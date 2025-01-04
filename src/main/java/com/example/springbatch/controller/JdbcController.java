package com.example.springbatch.controller;


import jakarta.mail.Flags;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@RestController
@RequestMapping("/jdbc")
public class JdbcController {

    private final JdbcTemplate jdbcTemplate;
    private final JavaMailSender mailSender;

    public JdbcController(JdbcTemplate jdbcTemplate, @Qualifier("mainDataSource") DataSource dataSource, JavaMailSender mailSender) {
        this.jdbcTemplate = jdbcTemplate;
        this.mailSender = mailSender;
        jdbcTemplate.setDataSource(dataSource);
    }


    @GetMapping("/insert")
    public ResponseEntity insert() throws IOException {
        List<String[]> data = csvParser();

        insertIntoDB(data);
        sendEmail("INSERTED");

        return ResponseEntity.ok().build();
    }

    private void insertIntoDB(List<String[]> data) {
        String sql = "INSERT INTO person (uuid, first_name, last_name, email, gender, date_time) VALUES (?, ?, ?, ?, ?, ?)";
        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                String[] item = data.get(i);
                ps.setString(1, item[0]);
                ps.setString(2, item[1]);
                ps.setString(3, item[2]);
                ps.setString(4, item[3]);
                ps.setString(5, item[4]);
                ps.setDate(6, new Date(System.currentTimeMillis()));
            }

            @Override
            public int getBatchSize() {
                return data.size();
            }
        });
    }

    public List<String[]> csvParser() throws IOException {
        List<String[]> result = new ArrayList<>();
        log.info("Parsing CSV");
        InputStream inputStream = new ClassPathResource("mock_data_1000.csv").getInputStream();
        CSVFormat csvFormat = CSVFormat.Builder.create(CSVFormat.DEFAULT)
                .setHeader("uuid", "first_name", "last_name", "email", "gender", "date_time")
                .setSkipHeaderRecord(true)
                .setDelimiter(',')
                .build();
        CSVParser parser = CSVParser.parse(inputStream, StandardCharsets.UTF_8, csvFormat);

        for (CSVRecord record : parser.getRecords()) {
            String[] row = new String[6];
            row[0] = record.get(0);
            row[1] = record.get(1);
            row[2] = record.get(2);
            row[3] = record.get(3);
            row[4] = record.get(4);
            row[5] = record.get(5);
            result.add(row);
        }
        log.info("CSV Parsed");
        return result;
    }


    @GetMapping("/update")
    public ResponseEntity update() throws IOException {
        List<String[]> stringList = csvParser();
        updateInDB(stringList);
        sendEmail("UPDATED");
        return ResponseEntity.ok().build();
    }


    public void updateInDB(List<String[]> stringList) {
        String sql = "UPDATE person SET date_time = ? WHERE uuid = ?";
        log.info("JDBC batch update...");
        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                ps.setDate(1, new Date(662970875000L));
                ps.setString(2, stringList.get(i)[0]);
            }

            @Override
            public int getBatchSize() {
                return stringList.size();
            }
        });
    }


    private void sendEmail(String mode) {

        MimeMessage mimeMessage = mailSender.createMimeMessage();
        try {
            mimeMessage.setFrom("Spring <MS_eHOJAO@trial-zr6ke4nkv3v4on12.mlsender.net>");
            mimeMessage.setRecipients(Message.RecipientType.TO, "Arun <arunshanagonda777@gmail.com>");
            mimeMessage.setSubject("Spring Jdbc Batch Example");
            mimeMessage.setFlag(Flags.Flag.FLAGGED, true);
            mimeMessage.setHeader("X-Priority", "1");
            String message = "Hello from <Strong>Spring Jdbc Batch</Strong> Example </br> \n";

            switch (mode) {
                case "INSERTED" -> {
                    message = message.concat("Data <h3>Inserted</h3> Successfully");
                    mimeMessage.setText(message, StandardCharsets.UTF_8.toString(), "html");
                }
                case "UPDATED" -> {
                    message = message.concat("Data <h3>Updated</h3> Successfully");
                    mimeMessage.setText(message, StandardCharsets.UTF_8.toString(), "html");
                }
            }

        } catch (MessagingException e) {
            throw new RuntimeException(e);
        }

        log.info("Sending email...");
        mailSender.send(mimeMessage);
    }

}
