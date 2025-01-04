package com.example.springbatch.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Date;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor

@Entity
@Table(name = "PERSON", schema = "spring_batch_example")
public class Person {

    @Id
    private UUID uuid;
    private String firstName;
    private String lastName;
    private String email;
    private String gender;
    private Date dateTime;

}
