//package com.example.springbatch.config;
//
//import com.example.springbatch.model.Person;
//import com.example.springbatch.repository.PersonRepository;
//import org.springframework.batch.core.Job;
//import org.springframework.batch.core.Step;
//import org.springframework.batch.core.job.builder.JobBuilder;
//import org.springframework.batch.core.repository.JobRepository;
//import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
//import org.springframework.batch.core.step.builder.StepBuilder;
//import org.springframework.batch.item.ItemProcessor;
//import org.springframework.batch.item.ItemWriter;
//import org.springframework.batch.item.data.RepositoryItemWriter;
//import org.springframework.batch.item.database.JdbcBatchItemWriter;
//import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
//import org.springframework.batch.item.file.FlatFileItemReader;
//import org.springframework.batch.item.file.LineMapper;
//import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
//import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
//import org.springframework.batch.item.file.mapping.DefaultLineMapper;
//import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
//import org.springframework.beans.factory.annotation.Qualifier;
//import org.springframework.boot.autoconfigure.batch.BatchDataSource;
//import org.springframework.boot.autoconfigure.batch.BatchDataSourceScriptDatabaseInitializer;
//import org.springframework.boot.context.properties.ConfigurationProperties;
//import org.springframework.boot.jdbc.DataSourceBuilder;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.context.annotation.Primary;
//import org.springframework.core.convert.ConversionService;
//import org.springframework.core.convert.converter.Converter;
//import org.springframework.core.convert.support.DefaultConversionService;
//import org.springframework.core.io.ClassPathResource;
//import org.springframework.transaction.PlatformTransactionManager;
//
//import javax.sql.DataSource;
//import java.sql.Date;
//import java.text.ParseException;
//import java.text.SimpleDateFormat;
//
//@Configuration
//public class BatchConfig {
//
//    private final DataSource dataSource;
//    private final DataSource batchDataSource;
//
//    private final PersonRepository personRepository;
//
//    public BatchConfig(DataSource dataSource, DataSource batchDataSource, PersonRepository personRepository) {
//        this.dataSource = dataSource;
//        this.batchDataSource = batchDataSource;
//        this.personRepository = personRepository;
//    }
//
//
//    @Bean
//    @Primary
//    @ConfigurationProperties("spring.datasource")
//    public DataSource mainDataSource() {
//        return DataSourceBuilder.create().build();
//    }
//
//    @Bean(name = "batchDataSource")
//    @BatchDataSource
//    @ConfigurationProperties("spring.batch.datasource")
//    public DataSource batchDataSource() {
//        return DataSourceBuilder.create().build();
//    }
//
//
//    @Bean
//    public FlatFileItemReader<Person> personItemReader() {
//        return new FlatFileItemReaderBuilder<Person>()
//                .name("personItemReader")
//                .resource(new ClassPathResource("mock_data_1000.csv"))
//                .lineMapper(lineMapper())
//                .linesToSkip(1)
//                .build();
//    }
//
//
////    @Bean
////    public JobRepository jobRepository(DataSource batchDataSource, PlatformTransactionManager transactionManager) throws Exception {
////        JobRepositoryFactoryBean jobRepositoryFactoryBean = new JobRepositoryFactoryBean();
////        jobRepositoryFactoryBean.setDataSource(batchDataSource);
////        jobRepositoryFactoryBean.setTransactionManager(transactionManager);
////        return jobRepositoryFactoryBean.getObject();
////    }
//
//    @Bean
//    public ItemProcessor<Person, Person> personItemProcessor() {
//        return new PersonItemProcessor();
//    }
//
//    @Bean
//    public ItemWriter<Person> personItemWriter() {
//        RepositoryItemWriter<Person> itemWriter = new RepositoryItemWriter<>();
//        itemWriter.setRepository(personRepository);
//        itemWriter.setMethodName("save");
//        return itemWriter;
//    }
//
//    @Bean
//    public JdbcBatchItemWriter<Person> jdbcBatchItemWriter() {
//        JdbcBatchItemWriterBuilder<Person> builder = new JdbcBatchItemWriterBuilder<>();
//        builder.dataSource(dataSource)
//                .sql("INSERT INTO person (uuid, first_name, last_name, email, gender, date_time) VALUES (:uuid, :firstName, :lastName, :email, :gender, :dateTime)")
//                .beanMapped();
//        return builder.build();
//    }
//
//    @Bean
//    public Step step1(JobRepository jobRepository, @Qualifier("transactionManager") PlatformTransactionManager platformTransactionManager) {
//        return new StepBuilder("step1", jobRepository)
//                .<Person, Person>chunk(250, platformTransactionManager)
//                .reader(personItemReader())
//                .writer(personItemWriter())
//                .processor(personItemProcessor())
//                .build();
//    }
//
//    @Bean
//    public Job job1(JobRepository jobRepository, @Qualifier("transactionManager") PlatformTransactionManager platformTransactionManager) {
//        return new JobBuilder("job1", jobRepository)
//                .flow(step1(jobRepository, platformTransactionManager))
//                .end()
//                .build();
//    }
//
//
//    private ConversionService conversionService() {
//        DefaultConversionService service = new DefaultConversionService();
//        DefaultConversionService.addDefaultConverters(service);
//        service.addConverter(new Converter<String, Date>() {
//            @Override
//            public Date convert(String source) {
//                try {
//                    java.util.Date parsed = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(source);
//                    return new Date(parsed.getTime());
//                } catch (ParseException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//        });
//        return service;
//    }
//
//    private LineMapper<Person> lineMapper() {
//        DefaultLineMapper<Person> lineMapper = new DefaultLineMapper<>();
//        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer(",");
//        tokenizer.setNames("uuid", "firstName", "lastName", "email", "gender", "dateTime");
//
//        BeanWrapperFieldSetMapper<Person> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
//        fieldSetMapper.setTargetType(Person.class);
//        fieldSetMapper.setConversionService(conversionService());
//
//        lineMapper.setLineTokenizer(tokenizer);
//        lineMapper.setFieldSetMapper(fieldSetMapper);
//        return lineMapper;
//    }
//
//}
