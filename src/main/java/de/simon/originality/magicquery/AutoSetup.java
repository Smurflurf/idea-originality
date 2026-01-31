package de.simon.originality.magicquery;

import de.simon.originality.Application;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Automatically determines the execution environment (JAR vs. IDE) on startup
 * and sets the correct base path for application resource files.
 * This implementation is robust against Spring Boot's "nested:" JAR protocol.
 */
@Service
public class AutoSetup {

    private static boolean isJar;
    private static Path dataDirectory;

    @PostConstruct
    public void init() {
        try {
            URL location = Application.class.getProtectionDomain().getCodeSource().getLocation();
            String locationStr = location.toString();

            // The most reliable way to check for a Spring Boot fat JAR environment.
            isJar = locationStr.contains(".jar");

            if (isJar) {
                System.out.println("Running from JAR environment detected.");

                int jarMarkerIndex = locationStr.indexOf(".jar");
                if (jarMarkerIndex == -1) {
                    throw new IllegalStateException("Could not find '.jar' marker in location: " + locationStr);
                }
                
                String jarPathStr = locationStr.substring(0, jarMarkerIndex + 3);

                // Remove the protocol prefix (e.g., "nested:" or "file:")
                System.out.println(jarPathStr);
                if (jarPathStr.contains(":")) {
                    jarPathStr = jarPathStr.substring(jarPathStr.indexOf(':') + 1);
                }

                // Remove prefixes
                jarPathStr = jarPathStr.replace("nested:", "");
                jarPathStr = jarPathStr.replace("jar:", "");
                jarPathStr = jarPathStr.replace("file:", "");
                
                
                Path jarFile = Paths.get(jarPathStr);
                Path parentDirectory = jarFile.getParent();

                if (parentDirectory == null) {
                	throw new RuntimeException("no parent dir found in jar Path.");
                }

                dataDirectory = parentDirectory.resolve("application-data");
                System.out.println("Data directory set to: " + dataDirectory.toAbsolutePath());

            } else {
                System.out.println("Running from IDE environment detected.");
                dataDirectory = Paths.get("src", "main", "python", "in_code_execution");
                System.out.println("Data directory set to: " + dataDirectory.toAbsolutePath());
            }

            if (!Files.isDirectory(dataDirectory)) {
                 System.err.println("FATAL: Data directory not found or not a directory: " + dataDirectory.toAbsolutePath());
                 System.exit(1);
            }

        } catch (Exception e) {
            System.err.println("FATAL: Could not determine application data directory.");
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Checks if the application is running from a JAR file.
     * @return true if running from a JAR, false otherwise.
     */
    public static boolean isJar() {
        return isJar;
    }

    /**
     * Gets the root directory where resource files (models, scripts, etc.) are located.
     * @return The absolute path to the data directory.
     */
    public static Path getDataDirectory() {
        return dataDirectory;
    }
}