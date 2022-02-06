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
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.data.elasticsearch.core.AggregationsContainer;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.clients.elasticsearch7.ElasticsearchAggregation;
import org.springframework.data.elasticsearch.core.cluster.ClusterHealth;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.Map;
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

    public List<SearchHit<StudentModel>> getSuggestions(String field, String query) {
        QueryBuilder qb = QueryBuilders.wildcardQuery(field,"*" + query + "*");
        SearchHits<StudentModel> suggestions = elasticTemplate.search(new NativeSearchQuery(qb),StudentModel.class);
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
}
