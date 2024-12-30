package com.example.springbatch.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

@Slf4j
@EnableAsync
@RestController
@RequestMapping("/batch")
public class BatchController {

    private final JobLauncher jobLauncher;
    private final Job job;
    private final JdbcTemplate jdbcTemplate;

    public BatchController(JobLauncher jobLauncher, Job job, JdbcTemplate jdbcTemplate) {
        this.jobLauncher = jobLauncher;
        this.job = job;
        this.jdbcTemplate = jdbcTemplate;
    }

    @GetMapping("/test")
    public ResponseEntity test() throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("jobName", "job1")
                .addLong("timestamp", System.currentTimeMillis())
                .toJobParameters();
        JobExecution jobExecution = jobLauncher.run(job, jobParameters);
        return ResponseEntity.ok(jobExecution.getStatus());
    }























//    @Transactional(propagation = Propagation.REQUIRED)
//    @GetMapping("/run")
//    public ResponseEntity<BatchStatus> run() throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException, SQLException {
//        JobParameters jobParameters = new JobParametersBuilder()
//                .addString("jobName", "job1")
//                .addLong("timestamp", System.currentTimeMillis())
//                .toJobParameters();
//
//        String sql = "INSERT INTO person (uuid, first_name, last_name, email, gender, date_time) VALUES (?, ?, ?, ?, ?, ?)";
////        String sql = "UPDATE person SET uuid = ?, first_name = ?, last_name = ?, email = ?, gender = ?, date_time = ? WHERE uuid = ?";
////        String sql = "UPDATE person SET date_time = ? WHERE gender = ?";
////        Connection connection = jdbcTemplate.getDataSource().getConnection();
////        connection.setAutoCommit(false);
////        PreparedStatement preparedStatement = connection.prepareStatement(sql);
////
////        for (int i = 1; i <= 5000; i++) {
////            preparedStatement.setString(1, UUID.randomUUID().toString());
////            preparedStatement.setString(2, "John" + i);
////            preparedStatement.setString(3, "Doe" + i);
////            preparedStatement.setString(4, i + "tW5V6@example.com");
////            preparedStatement.setString(5, i % 2 == 0 ? "Female" : "Male");
////            preparedStatement.setDate(6, new Date(System.currentTimeMillis()));
////            preparedStatement.addBatch();
////        }
////
////        preparedStatement.executeBatch();
////        connection.commit();
//
//        long start = System.currentTimeMillis();
//        log.info("Starting Jdbc batch update...");
//        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
//            @Override
//            public void setValues(PreparedStatement ps, int i) throws SQLException {
//                ps.setString(1, UUID.randomUUID().toString());
//                ps.setString(2, "John" + i);
//                ps.setString(3, "Doe" + i);
//                ps.setString(4, i + "tW5V6@example.com");
//                ps.setString(5, i % 2 == 0 ? "Female" : "Male");
//                ps.setDate(6, new Date(System.currentTimeMillis()));
////                ps.setDate(1, new Date(637180200000L));
////                ps.setString(2, "Male");
////                log.info("Processing record {}", i);
//            }
//
//            @Override
//            public int getBatchSize() {
//                return 10000;
//            }
//        });
//
//        log.info("Jdbc batch update completed...");
//        long end = System.currentTimeMillis();
//        log.info("Time taken: {} ms", end - start);
////        JobExecution jobExecution = jobLauncher.run(job, jobParameters);
////        return ResponseEntity.ok(jobExecution.getStatus());
//        return ResponseEntity.ok(BatchStatus.COMPLETED);
//    }
//
//
//    @Transactional(propagation = Propagation.REQUIRED)
//    @GetMapping("/update")
//    public ResponseEntity<BatchStatus> update() throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException, SQLException {
//        JobParameters jobParameters = new JobParametersBuilder()
//                .addString("jobName", "job1")
//                .addLong("timestamp", System.currentTimeMillis())
//                .toJobParameters();
//
//        updateJob();
//
////        String sql = "INSERT INTO person (uuid, first_name, last_name, email, gender, date_time) VALUES (?, ?, ?, ?, ?, ?)";
////        String sql = "UPDATE person SET date_time = ? WHERE gender = ?";
//
////        long start = System.currentTimeMillis();
////        log.info("Starting Jdbc batch update...");
////        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
////            @Override
////            public void setValues(PreparedStatement ps, int i) throws SQLException {
////                ps.setDate(1, new Date(637180200000L));
////                ps.setString(2, "Male");
////                log.info("Processing record {}", i);
////            }
////
////            @Override
////            public int getBatchSize() {
////                return 10000;
////            }
////        });
////
////        log.info("Jdbc batch update completed...");
////        long end = System.currentTimeMillis();
////        log.info("Time taken: {} ms", end - start);
//        return ResponseEntity.ok(BatchStatus.COMPLETED);
//    }
//
//    @Async
//    public void updateJob() {
//
////        String sql = "INSERT INTO person (uuid, first_name, last_name, email, gender, date_time) VALUES (?, ?, ?, ?, ?, ?)";
//        String sql = "UPDATE person SET date_time = ? WHERE gender = ?";
//
//        long start = System.currentTimeMillis();
//        log.info("Starting Jdbc batch update...");
//        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
//            @Override
//            public void setValues(PreparedStatement ps, int i) throws SQLException {
//                ps.setDate(1, new Date(637180200000L));
//                ps.setString(2, "Male");
//                log.info("Processing record {}", i);
//            }
//
//            @Override
//            public int getBatchSize() {
//                return 10000;
//            }
//        });
//
//        log.info("Jdbc batch update completed...");
//        long end = System.currentTimeMillis();
//        log.info("Time taken: {} ms", end - start);
//
//    }


}
