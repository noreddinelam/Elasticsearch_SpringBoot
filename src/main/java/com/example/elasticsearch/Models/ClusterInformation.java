package com.example.elasticsearch.Models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
@AllArgsConstructor
public class ClusterInformation {
    private final String clusterName;
    private final int nbShards;
    private final int nbNodes;
}
