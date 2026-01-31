package de.simon.originality.magicquery.visualization;

import java.util.List;
import java.util.Objects;

public class VisualizationLayer implements Comparable<VisualizationLayer> {

    private final String clusterId;
    private final int level;
    private final List<Double> position; 

    public VisualizationLayer(String clusterId, int level, List<Double> position) {
        this.clusterId = clusterId;
        this.level = level;
        this.position = position;
    }

    public String getClusterId() { return clusterId; }
    public int getLevel() { return level; }
    public List<Double> getPosition() { return position; }

    @Override
    public int compareTo(VisualizationLayer other) {
        return Integer.compare(this.level, other.level); 
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return Objects.equals(clusterId, ((VisualizationLayer) o).clusterId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterId);
    }
}