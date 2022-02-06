package com.example.elasticsearch;

import com.example.elasticsearch.Models.StudentModel;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ElasticRepo extends ElasticsearchRepository<StudentModel,String> {
}
