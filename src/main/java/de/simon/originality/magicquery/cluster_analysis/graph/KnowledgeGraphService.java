package de.simon.originality.magicquery.cluster_analysis.graph;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedPseudograph;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.simon.originality.magicquery.DataPathService;
import jakarta.annotation.PostConstruct;

/**
 * Service to load and query the cluster knowledge graph.
 * Provides an in-memory representation of the cluster hierarchy and methods to query its structure.
 */

@Service
public class KnowledgeGraphService {
	private static double xMin = 0, xMax = 1, yMin = 0, yMax = 1;
	private static Graph<String, DefaultEdge> graph;
	private static Map<String, GraphNode> nodeAttributesMap;

	public KnowledgeGraphService(DataPathService dataPathService) {
		// init after tempFilesService
	}


	///////////////////////////////////////////////////////////////////////////////////
	///										INIT									///
	///////////////////////////////////////////////////////////////////////////////////

	/**
	 * Loads the {@code knowledge_graph.json} from the temp directory and builds the in-memory JGraphT graph.
	 * Triggered on application startup after the constructor has been called.
	 */
	@PostConstruct
	public void initializeGraph() {
		System.out.println("Initializing Knowledge Graph...");
		graph = new DirectedPseudograph<>(DefaultEdge.class);
		nodeAttributesMap = new HashMap<>();

		try {
			Path graphPath = DataPathService.getKnowledgeGraphPath();

			if (graphPath == null || !Files.exists(graphPath)) {
				throw new IOException("Did not find Knowledge Graph file in: " + graphPath);
			}

			try (InputStream inputStream = Files.newInputStream(graphPath)) {
				ObjectMapper objectMapper = new ObjectMapper();
				GraphData graphData = objectMapper.readValue(inputStream, GraphData.class);

				for (GraphNode node : graphData.getNodes()) {
					graph.addVertex(node.getId());
					nodeAttributesMap.put(node.getId(), node);
				}

				for (GraphLink link : graphData.getLinks()) {
					if (graph.containsVertex(link.getSource()) && graph.containsVertex(link.getTarget())) {
						graph.addEdge(link.getSource(), link.getTarget());
					}
				}

				Optional<GraphNode> rootNode = getNodeAttributes("all_vectors");
				if (rootNode.isPresent() && rootNode.get().getEmbeddingBounds() != null) {
					Map<String, Double> bounds = rootNode.get().getEmbeddingBounds();
					xMin = bounds.getOrDefault("xmin", 0.0);
					xMax = bounds.getOrDefault("xmax", 1.0);
					yMin = bounds.getOrDefault("ymin", 0.0);
					yMax = bounds.getOrDefault("ymax", 1.0);
					System.out.println("Embedding-Grenzen erfolgreich aus Graphen geladen.");
				} else {
					System.err.println("WARNUNG: Keine Embedding-Grenzen im Root-Knoten des Graphen gefunden.");
				}

				System.out.println("Knowledge Graph successfully loaded with " + graph.vertexSet().size() + " nodes and " + graph.edgeSet().size() + " edges.");
			}

		} catch (IOException e) {
			System.err.println("Failed to load or parse knowledge_graph.json from the directory: " + DataPathService.getKnowledgeGraphPath());
			e.printStackTrace();
		}
	}


	///////////////////////////////////////////////////////////////////////////////////
	///							QUICK INFORMATION RETRIEVAL							///
	///////////////////////////////////////////////////////////////////////////////////
	
	/**
	 * Finds the smallest (i.e., most specific or deepest) common ancestor cluster for a given list of cluster IDs.
	 *
	 * @param clusterIds A list of cluster IDs for which to find the common ancestor.
	 * @return An {@link Optional} containing the ID of the smallest common cluster.
	 * Returns an empty Optional if the input list is null or empty.
	 * "all_vectors" is the ultimate fallback if no more specific cluster is found.
	 */
	public static Optional<String> findSmallestCommonCluster(List<String> clusterIds) {
		if (clusterIds == null || clusterIds.isEmpty()) {
			return Optional.empty();
		}

		// Get the ancestors of the first cluster, including the cluster itself.
		// This forms our initial set of potential common ancestors.
		Set<String> commonAncestors = new HashSet<>(
			getAncestors(clusterIds.get(0)).stream()
				.map(GraphNode::getId)
				.collect(Collectors.toSet())
		);

		// Iterate through the rest of the clusters and retain only the ancestors
		// that are also present in their hierarchy.
		for (int i = 1; i < clusterIds.size(); i++) {
			Set<String> currentAncestors = new HashSet<>(
				getAncestors(clusterIds.get(i)).stream()
					.map(GraphNode::getId)
					.collect(Collectors.toSet())
			);
			commonAncestors.retainAll(currentAncestors);
		}

		
		// From the remaining common ancestors, find the one that is deepest in the hierarchy.
		// The deepest cluster has the longest ID string (e.g., more hyphens).
		return commonAncestors.stream()
				.max(Comparator.comparingInt(String::length));
	}
	
	///////////////////////////////////////////////////////////////////////////////////
	///							QUICK INFORMATION RETRIEVAL							///
	///////////////////////////////////////////////////////////////////////////////////

	public static double getXMin() { return xMin; }
	public static double getXMax() { return xMax; }
	public static double getYMin() { return yMin; }
	public static double getYMax() { return yMax; }

	/**
	 * Retrieves the pre-calculated 2D position of a specific cluster centroid.
	 * @param nodeId The ID of the cluster.
	 * @return An {@link Optional} containing the list [x, y] of coordinates.
	 */
	public static Optional<List<Double>> getClusterPosition(String nodeId) {
		return getNodeAttributes(nodeId).map(GraphNode::getPos2d);
	}

	/**
	 * Retrieves the name of a specific cluster.
	 * @param nodeId The ID of the cluster.
	 * @return An {@link Optional} containing the cluster name.
	 */
	public static Optional<String> getClusterName(String nodeId) {
		return getNodeAttributes(nodeId).map(GraphNode::getClusterName);
	}

	/**
	 * Retrieves the size (number of members) of a specific cluster.
	 * @param nodeId The ID of the cluster.
	 * @return An {@link Optional} containing the cluster size.
	 */
	public static Optional<Integer> getClusterSize(String nodeId) {
		return getNodeAttributes(nodeId).map(GraphNode::getSize);
	}

	/**
	 * Retrieves the source distribution for a specific cluster.
	 * @param nodeId The ID of the cluster.
	 * @return An {@link Optional} containing the map of source distributions.
	 */
	public static Optional<Map<String, Integer>> getSourceDistribution(String nodeId) {
		return getNodeAttributes(nodeId).map(GraphNode::getSourceDistribution);
	}

	/**
	 * Retrieves the names of all direct children of a specific cluster.
	 * @param nodeId The ID of the parent cluster.
	 * @return A {@link Stream} of child cluster names.
	 */
	public static Stream<String> getChildrenNames(String nodeId) {
		return getChildren(nodeId).stream()
				.map(KnowledgeGraphService::getClusterName)
				.filter(Optional::isPresent)
				.map(Optional::get);
	}

	/**
	 * Finds all direct siblings of a given node.
	 * @param nodeId The ID of the node.
	 * @return A {@link List} of sibling node IDs.
	 */
	public static List<String> getSiblings(String nodeId) {
		if (graph == null || !graph.containsVertex(nodeId) || getParent(nodeId).isEmpty()) {
			return Collections.emptyList();
		}
		return getChildren((getParent(nodeId).get()))
				.stream()
				.filter(e -> !e.equals(nodeId))
				.collect(Collectors.toList());
	}

	///////////////////////////////////////////////////////////////////////////////////
	///								ON-GRAPH ANALYSIS								///
	///////////////////////////////////////////////////////////////////////////////////

	/**
	 * Retrieves all attributes for a given node ID.
	 * @param nodeId The ID of the cluster node.
	 * @return An {@link Optional} containing the {@link GraphNode}, or empty if not found.
	 */
	public static Optional<GraphNode> getNodeAttributes(String nodeId) {
		return Optional.ofNullable(nodeAttributesMap.get(nodeId));
	}

	/**
	 * Finds the direct parent of a given node.
	 * @param nodeId The ID of the child node.
	 * @return An {@link Optional} containing the parent's node ID.
	 */
	public static Optional<String> getParent(String nodeId) {
		if (graph == null || !graph.containsVertex(nodeId)) {
			return Optional.empty();
		}
		return graph.incomingEdgesOf(nodeId).stream()
				.map(graph::getEdgeSource)
				.findFirst();
	}

	/**
	 * Finds all direct children of a given node.
	 * @param nodeId The ID of the parent node.
	 * @return A {@link List} of child node IDs.
	 */
	public static List<String> getChildren(String nodeId) {
		if (graph == null || !graph.containsVertex(nodeId)) {
			return Collections.emptyList();
		}
		return graph.outgoingEdgesOf(nodeId).stream()
				.map(graph::getEdgeTarget)
				.collect(Collectors.toList());
	}

	/**
	 * Traces and returns the full path from the root to the specified node.
	 * @param nodeId The ID of the target node.
	 * @return An ordered {@link List} of {@link GraphNode} objects representing the path.
	 */
	public static List<GraphNode> getAncestors(String nodeId) {
		if (!nodeAttributesMap.containsKey(nodeId)) {
			return Collections.emptyList();
		}

		List<GraphNode> path = new java.util.ArrayList<>();
		Optional<String> currentId = Optional.of(nodeId);

		while (currentId.isPresent() && nodeAttributesMap.containsKey(currentId.get())) {
			path.add(nodeAttributesMap.get(currentId.get()));
			currentId = getParent(currentId.get());
		}

		Collections.reverse(path); 
		return path;
	}
}