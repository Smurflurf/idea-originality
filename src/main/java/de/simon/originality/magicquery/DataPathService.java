package de.simon.originality.magicquery;

import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;
import java.nio.file.Path;

/**
 * Provides static access to the absolute paths of required application data files.
 * The paths are determined at startup by the {@link AutoSetup} service.
 * This service replaces the need to copy files to a temporary directory.
 */
@Service
public class DataPathService {

    private static Path serverScriptPath;
    private static Path hdbscanModelsPath;
    private static Path imagesPath;
    private static Path umapModelPath;
    private static Path knowledgeGraphPath;

    /**
     * Injects the AutoSetup service to ensure it runs first and determines the base data directory.
     * @param autoSetup The auto-setup service.
     */
    public DataPathService(AutoSetup autoSetup) {
    }

    /**
     * Initializes all static path variables based on the data directory
     * identified by {@link AutoSetup}.
     */
    @PostConstruct
    public void initializePaths() {
        System.out.println("Initializing data file paths...");
        Path dataDir = AutoSetup.getDataDirectory();

        serverScriptPath = dataDir.resolve(MagicNumbers.VECTORIZER_SERVER_FILE_NAME.asString() + ".py");
        knowledgeGraphPath = dataDir.resolve(MagicNumbers.KNOWLEDGE_GRAPH_FILE_NAME.asString() + ".json");
        hdbscanModelsPath = dataDir.resolve(MagicNumbers.HDBSCAN_MODELS_FOLDER_PATH.asString());
        imagesPath = dataDir.resolve(MagicNumbers.IMAGES_FOLDER_PATH.asString());
        
        System.out.println("Python Server Path: " + serverScriptPath);
        System.out.println("UMAP Model Path: " + umapModelPath);
    }

    /**
     * Gets the absolute path to the Python vectorizer server script.
     * @return The path to the server script.
     */
    public static Path getServerScriptPath() {
        return serverScriptPath;
    }
    
    /**
     * Gets the absolute path to the knowledge graph JSON file.
     * @return The path to the knowledge graph.
     */
    public static Path getKnowledgeGraphPath() {
        return knowledgeGraphPath;
    }
    
    /**
     * Gets the absolute path to the directory containing the HDBSCAN model files.
     * @return The path to the HDBSCAN models directory.
     */
    public static Path getHdbscanModelsPath() {
        return hdbscanModelsPath;
    }
    
    /**
     * Gets the absolute path to the directory containing the png cluster images.
     * @return The path to the image directory.
     */
    public static Path getImagesPath() {
        return imagesPath;
    }
}