package de.simon.originality.magicquery.visualization;

import java.awt.Color;
import java.util.Objects;
import java.util.Optional;

import de.simon.originality.magicquery.cluster_analysis.graph.GraphNode;
import de.simon.originality.magicquery.cluster_analysis.graph.KnowledgeGraphService;

/**
 * A utility class for generating visually distinct, deterministic colors from string identifiers.
 */
public final class ColorFactory {
    /**
     * Generates a unique, deterministic, and visually pleasing color based on the attributes of a cluster.
     * This method uses multiple attributes from the Knowledge Graph to create a robust hash,
     * ensuring high visual contrast even for hierarchically or spatially close clusters.
     * It varies not only hue but also saturation and brightness for a richer color palette.
     *
     * @param clusterId The string ID of the cluster to generate a color for (e.g., "all_vectors-0-1").
     * @return A vibrant, deterministic {@link Color} object.
     */
    public static Color generateColor(String clusterId) {
        if ("all_vectors".equals(clusterId)) {
            return new Color(73, 73, 73); 
        }

        Optional<GraphNode> nodeOpt = KnowledgeGraphService.getNodeAttributes(clusterId);
        
        if (nodeOpt.isEmpty()) {
            int fallbackHash = clusterId.hashCode();
            float hue = (Math.abs(fallbackHash) % 360) / 360.0f;
            return Color.getHSBColor(hue, 0.7f, 0.9f);
        }
        
        GraphNode node = nodeOpt.get();
        int level = clusterId.split("-").length;
        int size = node.getSize();
        String parent = node.getParent();
        double persistence = node.getPersistenceScore();

        int combinedHash = Objects.hash(clusterId, level, size, parent, persistence);

        float hue = (float) (Math.abs(combinedHash) % 360) / 360.0f;
        float saturation = 0.75f + (float) (Math.abs(combinedHash >> 8) % 25) / 100.0f;
        float brightness = 0.9f + (float) (Math.abs(combinedHash >> 16) % 10) / 100.0f;

        return Color.getHSBColor(hue, saturation, brightness);
    }
}