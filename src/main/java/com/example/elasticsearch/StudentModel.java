package com.example.elasticsearch;

import lombok.Builder;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

@Builder
@Data
@Document(indexName="student")
public class StudentModel {
    @Id
    private final String id;
    @Field(type = FieldType.Text)
    private final String firstName;
    @Field(type = FieldType.Text)
    private final String lastName;
    @Field(type = FieldType.Text)
    private final String address;
    @Field(type = FieldType.Integer)
    private final int age;
}
