package com.example.elasticsearch;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.elasticsearch.core.SearchHit;
import static org.assertj.core.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

@SpringBootTest
class ElasticServiceTest {
//    @Autowired
//    private ElasticRepo elasticRepo;

    @Autowired
    private ElasticService elasticService;

    @Test
    void indexStudents() {
        List<StudentModel> listStudents = new ArrayList<>();
        listStudents.add(StudentModel.builder().firstName("zoubir").lastName("roubir").age(22).address("2 BD PABLO PICASSO, " +
                "94000 CRETEIL").build());
        listStudents.add(StudentModel.builder().firstName("thibo").lastName("kerdoiyuf").age(17).address("2 BD PABLO PICASSO, " +
                "94000 CRETEIL").build());
        List<StudentModel> list = elasticService.indexStudents(listStudents);
    }

    @Test
    void findMaturePersons() {
        List<SearchHit<StudentModel>> listSearchHits = elasticService.findMaturePersons();
        assertThat(listSearchHits).allMatch((searchHit) -> searchHit.getContent().getAge() > 18);
    }
}
