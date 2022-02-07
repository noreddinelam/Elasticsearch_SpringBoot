package com.example.elasticsearch;

import com.example.elasticsearch.Models.ClusterInformation;
import com.example.elasticsearch.Models.StudentModel;
import lombok.AllArgsConstructor;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.cluster.ClusterHealth;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Service
@AllArgsConstructor
public class ElasticService {
    private final ElasticRepo elasticRepo;
    private final ElasticsearchRestTemplate elasticTemplate;
    private final RestHighLevelClient client;

    public List<SearchHit<StudentModel>> findMaturePersons() {
        QueryBuilder qb = QueryBuilders.boolQuery().filter(QueryBuilders.rangeQuery("age").gte(18));
        SearchHits<StudentModel> searchHits = elasticTemplate.search(new NativeSearchQuery(qb), StudentModel.class);
        return searchHits.getSearchHits();
    }

    public List<StudentModel> getAllStudentsWithFieldOrderV1(String field, Sort.Direction sortDirection) {
        Sort sort = Sort.by(sortDirection, field);
        List<StudentModel> studentModel = new ArrayList<>();
        elasticRepo.findAll(sort).forEach(studentModel::add);
        return studentModel;
    }

    public List<SearchHit<StudentModel>> getAllStudentsWithFieldOrderV2(String field, SortOrder sortOrder) {
        NativeSearchQuery nsq = new NativeSearchQuery(null, null,
                Collections.singletonList(SortBuilders.fieldSort(field).order(sortOrder))
        );
        SearchHits<StudentModel> searchHits = elasticTemplate.search(nsq, StudentModel.class);
        return searchHits.getSearchHits();
    }

    public List<StudentModel> getAllStudentsWithFieldOrderV3(String field, SortOrder sortOrder) throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder().sort(SortBuilders.fieldSort(field).order(sortOrder));
        SearchRequest searchRequest =
                new SearchRequest().indices("student").source(builder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        return Arrays.stream(response.getHits().getHits()).map(org.elasticsearch.search.SearchHit::getSourceAsMap)
                .map(this::mapOfDocumentFieldsToStudentModelMapper)
                .collect(Collectors.toList());
    }

    public List<SearchHit<StudentModel>> getSuggestions(String field, String query) {
        QueryBuilder qb = QueryBuilders.wildcardQuery(field, "*" + query + "*");
        SearchHits<StudentModel> suggestions = elasticTemplate.search(new NativeSearchQuery(qb), StudentModel.class);
        return suggestions.getSearchHits();
    }

    public Map<String, Aggregation> getGroupsOfStudentsByAddress() throws IOException {
        TermsAggregationBuilder aggregation = AggregationBuilders.terms("addresses")
                .field("address");
        SearchSourceBuilder builder = new SearchSourceBuilder().aggregation(aggregation);
        SearchRequest searchRequest =
                new SearchRequest().indices("student").source(builder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        return response.getAggregations().asMap();
    }

    public Map<String, Aggregation> getGroupOfStudentsByAddressAndAverageAge() throws IOException {
        TermsAggregationBuilder aggregation = AggregationBuilders.terms("addresses")
                .field("address").subAggregation(AggregationBuilders.avg("AGE_AVG").field("age"));
        SearchSourceBuilder builder = new SearchSourceBuilder().aggregation(aggregation);
        SearchRequest searchRequest =
                new SearchRequest().indices("student").source(builder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        return response.getAggregations().asMap();
    }

    public double getAverageStudentsAge() throws IOException {
        AvgAggregationBuilder aggregation = AggregationBuilders.avg("AVG_AGE")
                .field("age");
        SearchSourceBuilder builder = new SearchSourceBuilder().aggregation(aggregation);
        SearchRequest searchRequest =
                new SearchRequest().indices("student").source(builder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        return response.getAggregations().<Avg>get("AVG_AGE").getValue();
    }

    public List<StudentModel> indexStudents(List<StudentModel> students) {
        return StreamSupport.stream(elasticRepo.saveAll(students).spliterator(), false).collect(Collectors.toList());
    }

    public ClusterInformation getClusterInformation() {
        ClusterHealth clusterHealth = this.elasticTemplate.cluster().health();
        return ClusterInformation.builder().clusterName(clusterHealth.getClusterName())
                .nbNodes(clusterHealth.getNumberOfNodes())
                .nbShards(clusterHealth.getActiveShards())
                .build();
    }

    /* NOT REQUEST */
    private StudentModel mapOfDocumentFieldsToStudentModelMapper(Map<String, Object> fields) {
        return StudentModel.builder().firstName((String) fields.get("firstname")).lastName((String) fields.get(
                "lastname")).age((Integer) fields.get("age")).address((String) fields.get("address")).build();
    }
}
