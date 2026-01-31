package de.simon.originality.magicquery.python;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.simon.originality.magicquery.AutoSetup;
import de.simon.originality.magicquery.DataPathService;
import de.simon.originality.magicquery.MagicNumbers;


class PythonServerTest {
	static PythonServerManager manager;
	static PythonService service;
	
	@Test
	@BeforeAll
	static void testStartServer() throws IOException {
		AutoSetup setup = new AutoSetup();
		setup.init();
		DataPathService data = new DataPathService(setup);
		data.initializePaths();
		manager = new PythonServerManager(data);
		service = new PythonService(new ObjectMapper(), manager);

		try {
			manager.restartServer();
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testIsPythonExecutablePresent() {
		assertNotNull(PythonServerUtils.findPythonExecutable());
	}

	@Test
	public void testSentenceTransformer() {
		service.vectorize("text");
	}
	
	@Test
	public void testUMAP() {
		service.reduceDimension(new float[MagicNumbers.VECTOR_SHAPE.asInteger()], "2d");
	}
	
	@AfterAll
	static void stopServer() {
		manager.stopServer();
	}
}
