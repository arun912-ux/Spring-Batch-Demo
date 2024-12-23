package com.example.springbatch.controller;

import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

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

    @GetMapping("/run")
    public ResponseEntity<BatchStatus> run() throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException, SQLException {
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("jobName", "job1")
                .addLong("timestamp", System.currentTimeMillis())
                .toJobParameters();

        Connection connection = jdbcTemplate.getDataSource().getConnection();
        connection.setAutoCommit(false);
        PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO person (uuid, first_name, last_name, email, gender, date_time) VALUES (?, ?, ?, ?, ?, ?)");

        for (int i = 1; i <= 5000; i++) {
            preparedStatement.setString(1, UUID.randomUUID().toString());
            preparedStatement.setString(2, "John" + i);
            preparedStatement.setString(3, "Doe" + i);
            preparedStatement.setString(4, i + "tW5V6@example.com");
            preparedStatement.setString(5, i % 2 == 0 ? "Female" : "Male");
            preparedStatement.setDate(6, new Date(System.currentTimeMillis()));
            preparedStatement.addBatch();
        }

        preparedStatement.executeBatch();
        connection.commit();
//        JobExecution jobExecution = jobLauncher.run(job, jobParameters);
//        return ResponseEntity.ok(jobExecution.getStatus());
        return ResponseEntity.ok(BatchStatus.COMPLETED);
    }

}
