//package com.example.springbatch.config;
//
//import com.example.springbatch.model.Person;
//import org.springframework.batch.item.ItemProcessor;
//
//public class PersonItemProcessor implements ItemProcessor<Person, Person> {
//    @Override
//    public Person process(Person item) {
//        Person out = new Person();
//        out.setUuid(item.getUuid());
//        out.setFirstName(item.getFirstName().toUpperCase());
//        out.setLastName(item.getLastName().toUpperCase());
//        out.setEmail(item.getEmail().toUpperCase());
//        out.setGender(item.getGender().toUpperCase());
//        out.setDateTime(item.getDateTime());
//        return out;
//    }
//}
