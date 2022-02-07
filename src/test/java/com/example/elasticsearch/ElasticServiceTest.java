package com.example.elasticsearch;

import com.example.elasticsearch.Models.ClusterInformation;
import com.example.elasticsearch.Models.StudentModel;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.SearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class ElasticServiceTest {

    @Autowired
    private ElasticService elasticService;

    private static List<StudentModel> listStudents = new ArrayList<>();

    @BeforeAll
    public static void initListStudents(){
        listStudents.add(StudentModel.builder().firstName("zoubir").lastName("roubir").age(22).address("2 BD PABLO " +
                "PICASSO, " +
                "94000 CRETEIL").build());
        listStudents.add(StudentModel.builder().firstName("thibo").lastName("kerdoiyuf").age(17).address("2 BD PABLO " +
                "PICASSO, " +
                "94000 CRETEIL").build());
        listStudents.add(StudentModel.builder().firstName("rashid").lastName("aliev").age(25).address("4 BD PABLO " +
                "PICASSO, " +
                "94000 CRETEIL").build());
        listStudents.add(StudentModel.builder().firstName("abdallah").lastName("idris").age(98).address("10 BD PABLO " +
                "PICASSO, " +
                "94000 CRETEIL").build());
        listStudents.add(StudentModel.builder().firstName("dasilva").lastName("junior").age(13).address("17 BD PABLO " +
                "PICASSO, " +
                "94000 CRETEIL").build());
        listStudents.add(StudentModel.builder().firstName("idk").lastName("idnk").age(25).address("17 BD PABLO " +
                "PICASSO, " +
                "94000 CRETEIL").build());
    }

    @Test
    void indexStudents() {
        List<StudentModel> list = elasticService.indexStudents(listStudents);
        assertThat(list).isEqualTo(listStudents);
    }

    @Test
    void findMaturePersons() {
        List<SearchHit<StudentModel>> listSearchHits = elasticService.findMaturePersons();
        assertThat(listSearchHits).allMatch((searchHit) -> searchHit.getContent().getAge() > 18);
    }

    @Test
    void getAllStudentsWithFieldOrderV1() {
        String field = "age";
        Sort.Direction direction = Sort.Direction.ASC;
        List<StudentModel> listStudents = elasticService.getAllStudentsWithFieldOrderV1(field,direction);
        List<Integer> expectedAddressesOrder = Arrays.asList(13,17,22,25,25,98);
        assertThat(listStudents.stream().map(StudentModel::getAge).collect(Collectors.toList())).isEqualTo(expectedAddressesOrder);
    }

    @Test
    void getAllStudentsWithFieldOrderV2() {
        String field = "age";
        SortOrder sortOrder = SortOrder.ASC;
        List<SearchHit<StudentModel>>  listStudents = elasticService.getAllStudentsWithFieldOrderV2(field,sortOrder);
        List<Integer> expectedAddressesOrder = Arrays.asList(13,17,22,25,25,98);
        assertThat(listStudents.stream().map((sh) -> sh.getContent().getAge()).collect(Collectors.toList())).isEqualTo(expectedAddressesOrder);
    }

    @Test
    void getAllStudentsWithFieldOrderV3(){
        String field = "age";
        SortOrder sortOrder = SortOrder.ASC;
        try {
            List<StudentModel>  listStudents = elasticService.getAllStudentsWithFieldOrderV3(field, sortOrder);
            assertThat(listStudents).containsExactlyInAnyOrder(listStudents.toArray(new StudentModel[0]));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Test
    void getSuggestionsUsingFirstname() {
        String field = "firstName";
        String query = "zou";
        List<SearchHit<StudentModel>> listSearchHits = elasticService.getSuggestions(field, query);
        System.out.println(listSearchHits.get(0).getContent());
        assertThat(listSearchHits).hasSize(1).allMatch((searchHit) -> searchHit.getContent().getFirstName().contains(query));
    }

    @Test
    void getGroupByAddressAggregation() {
        try {
            String aggregationName = "addresses";
            Map<String, Aggregation> aggregations = elasticService.getGroupsOfStudentsByAddress();
            assertThat(aggregations).hasSize(1);
            assertThat(aggregations.get(aggregationName).getName()).isEqualTo("addresses");
            ParsedStringTerms terms = (ParsedStringTerms) aggregations.get(aggregationName);
            List<String> actual =
                    terms.getBuckets().stream().map(MultiBucketsAggregation.Bucket::getKeyAsString).collect(Collectors.toList());
            assertThat(actual).hasSize(4).containsExactlyInAnyOrder("2 BD PABLO PICASSO, 94000 CRETEIL", "4 BD PABLO " +
                    "PICASSO, 94000 " +
                    "CRETEIL", "10 BD PABLO PICASSO, 94000 CRETEIL", "17 BD PABLO PICASSO, 94000 CRETEIL");
            Long l = terms.getBuckets().stream().map(MultiBucketsAggregation.Bucket::getDocCount).reduce(0L, Long::sum);
            assertThat(l).isEqualTo(6);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    void getGroupOfStudentsByAddressAndAverageAge() {
        try {
            String aggregationName = "addresses";
            String subAggregationName = "AGE_AVG";
            Map<String, Aggregation> aggregations = elasticService.getGroupOfStudentsByAddressAndAverageAge();
            assertThat(aggregations).hasSize(1);
            assertThat(aggregations.get(aggregationName).getName()).isEqualTo("addresses");
            ParsedStringTerms terms = (ParsedStringTerms) aggregations.get(aggregationName);
            List<Double> list =
                    terms.getBuckets().stream().map(MultiBucketsAggregation.Bucket::getAggregations).map((aggs) -> aggs.<Avg>get(subAggregationName).getValue()).collect(Collectors.toList());
            assertThat(list).containsExactlyInAnyOrder(19., 19.5, 98., 25.);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    void getAverageStudentsAge() {
        try {
            int avg = (int) (elasticService.getAverageStudentsAge());
            assertThat(avg).isEqualTo(33);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    void getClusterInformation() {
        ClusterInformation clusterInformation = elasticService.getClusterInformation();
        assertThat(clusterInformation.getClusterName()).isEqualTo("elasticsearch");
    }
}
