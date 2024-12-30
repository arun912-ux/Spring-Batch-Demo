
# Spring Batch Example

This project is a simple example of how to use Spring Batch to import data from a CSV file into a database.

## Requirements

* Java 17
* Spring Boot 3.3.7
* Spring Batch 5.0.0
* H2 Database
* Mariadb 

## How to run

1. Clone the project
2. Run `./gradlew bootRun` to start the application
3. The application will import the data from the CSV file located in the `src/main/resources` folder into the H2 database
4. You can access the H2 console at `http://localhost:8080/h2-console` with the username `sa` and password `` (empty string)

## How it works

The application uses Spring Batch to create a job that reads from the CSV file and writes to the database.

The job is configured in the `BatchConfig` class, which defines the steps of the job. The steps are:

1. Read from the CSV file using a `FlatFileItemReader`
2. Process the data using a custom `ItemProcessor`
3. Write to the database using a `JdbcBatchItemWriter`

The job is launched by the `JobLauncher` which is configured in the `application.properties` file.

The application also uses Spring Data JPA to persist the data to the database.

## Notes

* The CSV file is read from the `src/main/resources` folder
* The database is an in-memory H2 database
* The application is configured to log the progress of the job
* The application is configured to restart the job if it fails
* The application is configured to skip the job if it has already been run successfully before