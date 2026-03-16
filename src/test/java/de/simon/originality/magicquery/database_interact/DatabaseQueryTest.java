package de.simon.originality.magicquery.database_interact;

import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.simon.originality.magicquery.MagicNumbers;
import de.simon.originality.magicquery.cluster_analysis.graph.KnowledgeGraphService;
import de.simon.originality.security.HtmlSanitizerService;

class DatabaseQueryTest {
	DatabaseQuery query;
	@Test
	void testInit() {
		query.init();

		if(!query.ping())
			fail("Server IP is null. System environment variable 'QDRANT_HOST' is not set.");
	}

	@Test
	void testQuery() {
		query.init();
		float[] vector = new float[MagicNumbers.VECTOR_SHAPE.asInteger()];
		var result = query.search(MagicNumbers.QDRANT_VECTOR_COLLECTION_NAME.asString(), vector, 1);
		if(result.getFirst() == null)
			fail("Database is empty or query went missing. Null was returned.");
	}

	@AfterEach
	void shutdown() {
		query.cleanup();
	}
	
	@BeforeEach
	void init() {
		query = new DatabaseQuery(new LatexConverter(), new HtmlSanitizerService(), new KnowledgeGraphService(null));
	}
}
