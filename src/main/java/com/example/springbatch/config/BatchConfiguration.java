package com.example.springbatch.config;

import com.example.springbatch.model.Person;
import jakarta.annotation.Nonnull;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.ArrayFieldSetMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DefaultFieldSet;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.validation.BindException;

import javax.sql.DataSource;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.UUID;

@Configuration
public class BatchConfiguration {

    private final DataSource dataSource;
    private final DataSource jobDataSource;


    public BatchConfiguration(@Qualifier("mainDataSource") DataSource dataSource, DataSource jobDataSource) {
        this.dataSource = dataSource;
        this.jobDataSource = jobDataSource;
    }


    @Bean
    public JdbcBatchItemWriter<Person> jdbcBatchItemWriter() {
        JdbcBatchItemWriterBuilder<Person> builder = new JdbcBatchItemWriterBuilder<>();
        builder.dataSource(dataSource)
                .sql("INSERT INTO person (uuid, first_name, last_name, email, gender, date_time) VALUES (:uuid, :firstName, :lastName, :email, :gender, :dateTime)")
                .beanMapped();
        return builder.build();
    }

    @Bean
    public ItemProcessor<String[], Person> itemProcessor() {
        ItemProcessor<String[], Person> processor = new ItemProcessor<>() {
            @Override
            public Person process(@Nonnull String[] items) throws ParseException {
                Person person = new Person();
                person.setUuid(UUID.fromString(items[0]));
                person.setFirstName(items[1].toUpperCase());
                person.setLastName(items[2].toUpperCase());
                person.setEmail(items[3]);
                person.setGender(items[4]);
                java.util.Date parsed = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(items[5]);
                person.setDateTime(new Date(parsed.getTime()));
                return person;
            }
        };
        return processor;
    }


    @Bean
    public ItemReader<String[]> itemReader() throws BindException {
        FieldSetMapper<String[]> fieldSetMapper = new ArrayFieldSetMapper();
        FieldSet fieldSet1 = new DefaultFieldSet(new String[]{"uuid", "firstName", "lastName", "email", "gender", "dateTime"});
        fieldSetMapper.mapFieldSet(fieldSet1);
        FlatFileItemReader<String[]> build = new FlatFileItemReaderBuilder<String[]>()
                .name("personItemReader")
                .resource(new ClassPathResource("mock_data_1000.csv"))
                .delimited()
                .names("uuid", "firstName", "lastName", "email", "gender", "dateTime")
                .linesToSkip(1)
                .fieldSetMapper(fieldSet -> new String[]{
                        fieldSet.readString("uuid"),
                        fieldSet.readString("firstName"),
                        fieldSet.readString("lastName"),
                        fieldSet.readString("email"),
                        fieldSet.readString("gender"),
                        fieldSet.readString("dateTime")
                })
//                .fieldSetMapper(fieldSet -> fieldSet.readString("uuid"))
//                .fieldSetMapper(fieldSetMapper)
                .build();
        return build;
    }


    @Bean
    public Step step1(JobRepository jobRepository, PlatformTransactionManager transactionManager) throws BindException {
        return new StepBuilder("step1", jobRepository)
                .<String[], Person>chunk(250, transactionManager)
                .reader(itemReader())
                .processor(itemProcessor())
                .writer(jdbcBatchItemWriter())
                .build();
    }

    public Step step2(JobRepository jobRepository, PlatformTransactionManager transactionManager) throws BindException {
        return new StepBuilder("step2", jobRepository)
                .<String[], Person>chunk(50, transactionManager)
                .reader(itemReader())
                .processor(itemProcessor())
                .writer(jdbcBatchItemWriter())
                .build();
    }

    @Bean
    public Job job1(JobRepository jobRepository, PlatformTransactionManager transactionManager) throws BindException {
        return new JobBuilder("job1", jobRepository)
                .start(step1(jobRepository, transactionManager))
                .build();
    }


    public Job job2(JobRepository jobRepository, PlatformTransactionManager transactionManager) throws BindException {
        return new JobBuilder("job2", jobRepository)
                .start(step2(jobRepository, transactionManager))
                .build();
    }

}
