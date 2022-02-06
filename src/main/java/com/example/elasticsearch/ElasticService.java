package com.example.elasticsearch;

import com.example.elasticsearch.Models.ClusterInformation;
import com.example.elasticsearch.Models.StudentModel;
import lombok.AllArgsConstructor;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.cluster.ClusterHealth;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Service
@AllArgsConstructor
public class ElasticService {
    private final ElasticRepo elasticRepo;
    private final ElasticsearchRestTemplate elasticTemplate;

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
