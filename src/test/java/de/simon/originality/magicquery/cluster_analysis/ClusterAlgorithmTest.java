package de.simon.originality.magicquery.cluster_analysis;

import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.simon.originality.magicquery.MagicNumbers;
import de.simon.originality.magicquery.cluster_analysis.graph.KnowledgeGraphService;
import de.simon.originality.magicquery.database_interact.DatabaseQuery;
import de.simon.originality.magicquery.database_interact.DatabaseQuery.QueryResult;
import de.simon.originality.security.HtmlSanitizerService;
import de.simon.originality.magicquery.database_interact.LatexConverter;

public class ClusterAlgorithmTest {
	ClusterAlgorithm clusti;
	DatabaseQuery query;
	
	@Test
	void testSerendipity() {
		float[] vector = new float[MagicNumbers.VECTOR_SHAPE.asInteger()];
		List<QueryResult> allNeighborClusterResults = query
				.search(MagicNumbers.QDRANT_CENTROID_COLLECTION_NAME.asString(), vector, MagicNumbers.N_NEAREST_CLUSTERS_FOR_ANALYSIS.asInteger());
		
		Map<String, Float> serendipitousClusters = ClusterAlgorithm.findSerendipitousClusters(allNeighborClusterResults, vector, MagicNumbers.N_NEAREST_CLUSTERS_FOR_FRONTEND.asInteger());

		if(serendipitousClusters.keySet().size() > 0)
			return;
		
		fail("No serendipitous clusters found! Changes in QDrant API?");
	}
	
	@AfterEach
	void cleanUp() {
		query.cleanup();
	}
	
	@BeforeEach
	void init() {
		query = new DatabaseQuery(new LatexConverter(), new HtmlSanitizerService(), new KnowledgeGraphService(null));
		query.init();
		clusti = new ClusterAlgorithm(query);
	}
}
