package de.simon.originality.magicquery.vectorizer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.simon.originality.magicquery.MagicNumbers;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;


/**
 * Starts a Python server by executing a Python script.
 * Embedding sentences into the vector space gets handled by that Python server, so this is the interface.
 * Contains different security measures to ensure a stable Python server start.
 * Extracts the Python script to the systems TEMP folder, starts it from there, to ensure a save start even from jars.
 * If the server does not start, "python" and "python3" get exchanged as command-line arguments.
 * If the server does start, it must ping back once, to ensure everything is healthy.
 */
@Service
public class Vectorizer {
	private Process pythonProcess;
	private final WebClient webClient;
	private final ObjectMapper objectMapper;
    private Path tempScriptPath;
    
	public Vectorizer(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
		this.webClient = WebClient.builder()
				.baseUrl("http://127.0.0.1:5001")
				.build();
	}

	/**
	 * Sends text via HTTP to the Python server and returns the resulting vector.
	 * @param text text to vectorize.
	 * @return A float[] representing the vector.
	 */
	public float[] vectorize(String text) {
		try {
			String responseJson = webClient.post()
					.uri("/vectorize")
					.bodyValue(text)
					.retrieve()
					.bodyToMono(String.class)
					.block(Duration.ofSeconds(MagicNumbers.WAIT_FOR_VECTORIZING_SERVER_ANSWER_MS.asInteger()));

			if (responseJson == null) {
				throw new RuntimeException("Vectorize-server didn't answer.");
			}

			JsonNode rootNode = objectMapper.readTree(responseJson);
			JsonNode vectorNode = rootNode.path("vector");

			return objectMapper.treeToValue(vectorNode, float[].class);

		} catch (Exception e) {
			throw new RuntimeException("Error communicating with the vectorize-server", e);
		}
	}
	
	/**
	 * Extracts the Python server script to the systems TEMP folder.
	 * @throws IOException
	 */
	private void extractPythonServer() throws IOException {
		try {
            Resource scriptResource = new ClassPathResource("python" + File.separator + MagicNumbers.VECTORIZER_SERVER_FILE_NAME.asString()+".py");
            if (!scriptResource.exists()) {
                throw new FileNotFoundException("Could not find python" + File.separator + MagicNumbers.VECTORIZER_SERVER_FILE_NAME.asString()+".py in classpath.");
            }
            
            this.tempScriptPath = Files.createTempFile("temp_"+MagicNumbers.VECTORIZER_SERVER_FILE_NAME.asString(), ".py");
            
            try (InputStream inputStream = scriptResource.getInputStream()) {
                Files.copy(inputStream, tempScriptPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            }
            System.out.println("Python script was temorarily copied to: " + tempScriptPath.toAbsolutePath());

        } catch (IOException e) {
            throw new IOException("Error extracting the python-skript from the jar.", e);
        }
	}

	/**
	 * Starts the Python server.
	 * @throws IOException
	 */
	@PostConstruct
	public void startServer() throws IOException {
		System.out.println("Starting python-vektorizer-server...");

		extractPythonServer();
		
		String[] pythonCommands = {"python3", "python"};

		Process process = null;

		try (ServerSocket callbackSocket = new ServerSocket(0)) {

			int callbackPort = callbackSocket.getLocalPort();
			System.out.println("Temporary callback-server listening on port: " + callbackPort);

			callbackSocket.setSoTimeout(MagicNumbers.WAIT_FOR_PYTHON_PROCESS_START_MS.asInteger());

			for (String command : pythonCommands) {
				try {
					ProcessBuilder pb = new ProcessBuilder(
							command, 
							tempScriptPath.toAbsolutePath().toString(), 
							Integer.toString(callbackPort));
					pb.directory(this.tempScriptPath.getParent().toFile());
					process = pb.start();

					if (process.waitFor(MagicNumbers.WAIT_FOR_PYTHON_PROCESS_CRASH_MS.asInteger(), TimeUnit.MILLISECONDS)) {
						int exitCode = process.exitValue();
						System.err.println("Start with '" + command + "' crashed instantly with exit-code " + exitCode + ".");
						process = null;
						continue;
					}

					System.out.println("Process was started succesfully with '" + command + "'.");
					this.pythonProcess = process;
					break;

				} catch (IOException e) {
					e.printStackTrace();
					process = null;
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			if (this.pythonProcess == null) {
				throw new IOException("Server could not be started.");
			}

			System.out.println("Waiting for python-processes 'ready'-signal...");
            try (Socket clientSocket = callbackSocket.accept()) {
                System.out.println("'Ready'-signal received on port " + clientSocket.getLocalPort());
                return; 
            } catch (IOException e) {
            	throw new IOException("Python-server timed out. \n" + e.getMessage());
            }
		}
	}

	/**
	 * Shuts down the Python server.
	 */
	@PreDestroy
	public void stopServer() {
		System.out.println("Shutting python-vectorizer-server down...");
		if (pythonProcess != null && pythonProcess.isAlive()) {
			pythonProcess.destroy();
			try {
				if (!pythonProcess.waitFor(5, java.util.concurrent.TimeUnit.SECONDS)) {
					System.err.println("Python-vectorizer-server didn't shut down, destroying it forcibly...");
					pythonProcess.destroyForcibly();
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			System.out.println("Python-vectorizer-server shut down.");
		}
		
		if (tempScriptPath != null) {
            try {
                Files.deleteIfExists(tempScriptPath);
                System.out.println("Deleted temporary python-script file.");
            } catch (IOException e) {
                System.err.println("Error deleting temporary python-script file: " + e.getMessage());
            }
        }
	}
}