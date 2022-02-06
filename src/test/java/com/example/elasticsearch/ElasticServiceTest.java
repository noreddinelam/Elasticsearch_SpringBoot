package com.example.elasticsearch;

import com.example.elasticsearch.Models.ClusterInformation;
import com.example.elasticsearch.Models.StudentModel;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.elasticsearch.core.SearchHit;
import static org.assertj.core.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

@SpringBootTest
class ElasticServiceTest {

    @Autowired
    private ElasticService elasticService;

    @Test
    void indexStudents() {
        List<StudentModel> listStudents = new ArrayList<>();
        listStudents.add(StudentModel.builder().firstName("zoubir").lastName("roubir").age(22).address("2 BD PABLO PICASSO, " +
                "94000 CRETEIL").build());
        listStudents.add(StudentModel.builder().firstName("thibo").lastName("kerdoiyuf").age(17).address("2 BD PABLO PICASSO, " +
                "94000 CRETEIL").build());
        listStudents.add(StudentModel.builder().firstName("rashid").lastName("aliev").age(25).address("4 BD PABLO PICASSO, " +
                "94000 CRETEIL").build());
        listStudents.add(StudentModel.builder().firstName("abdallah").lastName("idris").age(98).address("10 BD PABLO PICASSO, " +
                "94000 CRETEIL").build());
        listStudents.add(StudentModel.builder().firstName("dasilva").lastName("junior").age(13).address("17 BD PABLO PICASSO, " +
                "94000 CRETEIL").build());
        listStudents.add(StudentModel.builder().firstName("idk").lastName("idnk").age(25).address("17 BD PABLO PICASSO, " +
                "94000 CRETEIL").build());
        List<StudentModel> list = elasticService.indexStudents(listStudents);
        assertThat(list).isEqualTo(listStudents);
    }

    @Test
    void findMaturePersons() {
        List<SearchHit<StudentModel>> listSearchHits = elasticService.findMaturePersons();
        assertThat(listSearchHits).allMatch((searchHit) -> searchHit.getContent().getAge() > 18);
    }

    @Test
    void getSuggestionsUsingFirstname() {
        String field = "firstName";
        String query = "zou";
        List<SearchHit<StudentModel>> listSearchHits = elasticService.getSuggestions(field,query);
        System.out.println()
        assertThat(listSearchHits).hasSize(1).allMatch((searchHit) -> searchHit.getContent().getFirstName().contains(query));
    }

    @Test
    void getClusterInformation(){
        ClusterInformation clusterInformation = elasticService.getClusterInformation();
        assertThat(clusterInformation.getClusterName()).isEqualTo("elasticsearch");
    }
}
