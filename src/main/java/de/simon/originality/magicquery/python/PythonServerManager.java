package de.simon.originality.magicquery.python;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;

import org.springframework.stereotype.Service;

import de.simon.originality.magicquery.DataPathService;
import de.simon.originality.magicquery.MagicNumbers;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

/**
 * Starts a Python server by executing a Python script.
 * Embedding sentences into the vector space gets handled by that Python server.
 * Contains different security measures to ensure a stable Python server start.
 * Extracts the Python script to the systems TEMP folder, starts it from there, to ensure a save start even from jars.
 * If the server does start it must ping back once to ensure it is healthy and running.
 */
@Service
public class PythonServerManager {
    private Process pythonProcess;
    private final Object lock = new Object();

    public PythonServerManager(DataPathService dataFilesService) {
        // init after tempFilesService
    }
    
    @PostConstruct
    public void initialStart() {
        try {
            startServer();
        } catch (IOException e) {
            System.err.println("FATAL: Python server could not be started on initial launch. Application might not work correctly.");
            e.printStackTrace();
        }
    }
    
    
    private void startServer() throws IOException {
        synchronized (lock) {
            // Wenn schon ein Prozess läuft, nichts tun.
            if (pythonProcess != null && pythonProcess.isAlive()) {
                System.out.println("Python server is already running.");
                return;
            }
            
            System.out.println("Attempting to start python-vectorizer-server...");
            String command = PythonServerUtils.findPythonExecutable();

            try (ServerSocket callbackSocket = new ServerSocket(0)) {
                int callbackPort = callbackSocket.getLocalPort();
                callbackSocket.setSoTimeout(MagicNumbers.WAIT_FOR_PYTHON_PROCESS_START_MS.asInteger());

                ProcessBuilder pb = new ProcessBuilder(
                        command, 
                        DataPathService.getServerScriptPath().toAbsolutePath().toString(), 
                        Integer.toString(callbackPort));
                pb.directory(DataPathService.getServerScriptPath().getParent().toFile());

                Path parentDir = DataPathService.getServerScriptPath().getParent();
                Path logDir = parentDir.resolve("logs");

                try {
                    Files.createDirectories(logDir);
                } catch (IOException e) {
                    System.err.println("WARNUNG: Konnte das Log-Verzeichnis nicht erstellen: " + logDir.toAbsolutePath());
                    e.printStackTrace();
                }

                File pythonLogFile = logDir.resolve("python_server_"+System.currentTimeMillis()+".log").toFile();
                pb.redirectErrorStream(true);
                pb.redirectOutput(ProcessBuilder.Redirect.appendTo(pythonLogFile));
                
                this.pythonProcess = pb.start();


                System.out.println("Waiting for python-process 'ready'-signal...");
                try (Socket clientSocket = callbackSocket.accept()) {
                    System.out.println("Python server started successfully and sent ready signal.");
                } catch (IOException e) {
                    this.pythonProcess.destroyForcibly();
                    throw new IOException("Python-server timed out after starting.", e);
                }
            }
        }
    }
    
    public void restartServer() throws IOException {
        System.out.println("Restarting Python server...");
        stopServer();
        startServer(); 
    }

    public boolean isServerAlive() {
        return pythonProcess != null && pythonProcess.isAlive();
    }
    
    @PreDestroy
    public void stopServer() {
        synchronized (lock) {
            System.out.println("Shutting down python-vectorizer-server...");
            if (pythonProcess != null) {
                if (pythonProcess.isAlive()) {
                    pythonProcess.destroy();
                    try {
                        if (!pythonProcess.waitFor(5, java.util.concurrent.TimeUnit.SECONDS)) {
                            System.err.println("Python-server didn't shut down gracefully, destroying forcibly.");
                            pythonProcess.destroyForcibly();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                pythonProcess = null;
                System.out.println("Python-server process terminated.");
            }
        }
    }
}