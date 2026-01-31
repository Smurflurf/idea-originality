package de.simon.originality.magicquery.cluster_analysis.graph;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class GraphNode {
    private String id;
    private String parent;
    private int size;
    
    @JsonProperty("cluster_name")
    private String clusterName;

    @JsonProperty("cluster_description")
    private String clusterDescription;
    
    @JsonProperty("persistence_score")
    private double persistenceScore;

    @JsonProperty("coverage_of_parent_%")
    private double coverageOfParentPercentage;
    
    @JsonProperty("source_distribution")
    private Map<String, Integer> sourceDistribution;

    @JsonProperty("pos_2d")
    private List<Double> pos2d;
    
    @JsonProperty("embedding_bounds")
    private Map<String, Double> embeddingBounds;
    
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getParent() { return parent; }
    public void setParent(String parent) { this.parent = parent; }
    public int getSize() { return size; }
    public void setSize(int size) { this.size = size; }
    public String getClusterName() { return clusterName; }
    public void setClusterName(String clusterName) { this.clusterName = clusterName; }
    public String getClusterDescription() { return clusterDescription; }
    public void setClusterDescription(String clusterDescription) { this.clusterDescription = clusterDescription; }
    public double getPersistenceScore() { return persistenceScore; }
    public void setPersistenceScore(double persistenceScore) { this.persistenceScore = persistenceScore; }
    public double getCoverageOfParentPercentage() { return coverageOfParentPercentage; }
    public void setCoverageOfParentPercentage(double coverage) { this.coverageOfParentPercentage = coverage; }
    public Map<String, Integer> getSourceDistribution() { return sourceDistribution; }
    public void setSourceDistribution(Map<String, Integer> dist) { this.sourceDistribution = dist; }
    public List<Double> getPos2d() { return pos2d; }
    public void setPos2d(List<Double> pos2d) { this.pos2d = pos2d; }
    public Map<String, Double> getEmbeddingBounds() { return embeddingBounds; }
    public void setEmbeddingBounds(Map<String, Double> bounds) { this.embeddingBounds = bounds; }
}