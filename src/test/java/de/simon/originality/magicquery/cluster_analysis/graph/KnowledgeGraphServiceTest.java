package de.simon.originality.magicquery.cluster_analysis.graph;

import java.io.IOException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import de.simon.originality.magicquery.AutoSetup;
import de.simon.originality.magicquery.DataPathService;


@TestInstance(Lifecycle.PER_CLASS)
class KnowledgeGraphServiceTest {
	DataPathService data;
	KnowledgeGraphService kg;
	
	@Test
	void testAccess() {
		String cluster_id = "all_vectors-0";
		System.out.println("["+cluster_id+"] : " + kg.getClusterName(cluster_id).get());
	}
	
	@BeforeAll
	void init() throws IOException {
		var setup = new AutoSetup();
		setup.init();
		var data = new DataPathService(setup);
		data.initializePaths();
		kg = new KnowledgeGraphService(data);
		kg.initializeGraph();
	}
}
