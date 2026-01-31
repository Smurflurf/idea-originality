package de.simon.originality.magicquery.cluster_analysis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;

import com.google.common.primitives.Floats;

import de.simon.originality.magicquery.MagicNumbers;
import de.simon.originality.magicquery.cluster_analysis.graph.KnowledgeGraphService;
import de.simon.originality.magicquery.database_interact.DatabaseQuery;
import de.simon.originality.magicquery.database_interact.DatabaseQuery.QueryResult;

@Service
public class ClusterAlgorithm {
    private static DatabaseQuery databaseQuery;
    
    /**
     * Konstruktor, der von Spring aufgerufen wird, um die Abhängigkeit
     * zur DatabaseQuery-Klasse zu injizieren.
     * @param databaseQuery Die Instanz des Datenbank-Query-Services.
     */
    public ClusterAlgorithm(DatabaseQuery databaseQuery) {
        ClusterAlgorithm.databaseQuery = databaseQuery;
    }

    /**
     * Hauptmethode des "Gesteuerten Centroid-Sonden"-Algorithmus.
     * Findet serendipitische Cluster, indem er relevante thematische Säulen identifiziert
     * und innerhalb dieser Säulen nach den für die Nutzeranfrage relevantesten Clustern sucht.
     *
     * @param allNeighborClusters Die vollständige, nach Relevanz sortierte Liste der Nachbarcluster (z.B. Top 250).
     * @param userVector          Der 1024D Vektor der ursprünglichen Nutzeranfrage.
     * @param numTopToExclude     Die Anzahl der zu ignorierenden Top-Treffer, um die "Serendipity-Zone" zu definieren.
     * @param alpha               Der Steuerungsfaktor (0.0 bis 1.0), wie stark der userVector die Sonde beeinflusst.
     * @return Eine Liste von Cluster-IDs der serendipitischen Gewinner.
     */
    public static Map<String, Float> findSerendipitousClusters(
            List<QueryResult> allNeighborClusters,
            float[] userVector,
            int numTopToExclude) {

        // Schritt 1 & 2: Definiere die "Serendipity-Zone", indem die direktesten Treffer übersprungen werden.
        if (allNeighborClusters == null || allNeighborClusters.size() <= numTopToExclude) {
            return Collections.emptyMap();
        }
        List<QueryResult> serendipityPool = allNeighborClusters.stream()
                .skip(numTopToExclude)
                .collect(Collectors.toList());
        
        if (serendipityPool.isEmpty()) {
            return Collections.emptyMap();
        }

        // Schritt 3: Finde den kleinsten gemeinsamen Kontext (Vorfahren-Cluster) für den Serendipity-Pool.
        List<String> poolIds = serendipityPool.stream()
                                              .map(qr -> (String) qr.payload().get("id"))
                                              .collect(Collectors.toList());
        Optional<String> commonAncestorOpt = findSmallestCommonCluster(poolIds);
        
        if (!commonAncestorOpt.isPresent()) {
            return Collections.emptyMap(); // Kein gemeinsamer Kontext gefunden.
        }
        String commonAncestorId = commonAncestorOpt.get();

        // Schritt 4: Identifiziere die thematischen "Säulen" (direkte Kinder des Kontextes).
        List<String> mainBranches = KnowledgeGraphService.getChildren(commonAncestorId);
        if (mainBranches.isEmpty()) { 
            mainBranches = List.of(commonAncestorId);
       }

        // Schritt 5: Führe die GESTEUERTEN Centroid-Sonden-Abfragen für jede Säule durch.
        Map<String, Float> winnerMap = new HashMap<>();
        for (String branchId : mainBranches) {
            
            // Finde alle Cluster im Pool, die zu dieser spezifischen Säule gehören.
            List<QueryResult> membersOfBranch = serendipityPool.stream()
                .filter(qr -> ((String) qr.payload().get("id")).startsWith(branchId))
                .collect(Collectors.toList());

            if (membersOfBranch.isEmpty()) {
                continue; // Diese Säule hat keine Relevanz für die aktuelle Anfrage.
            }

            // Extrahiere die Vektoren dieser relevanten Mitglieder.
            List<float[]> vectorsInBranch = new ArrayList<>();
            for (QueryResult member : membersOfBranch) {
                Object vectorObj = member.payload().get("vector");
                if (vectorObj instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<Number> vectorAsList = (List<Number>) vectorObj;
                    vectorsInBranch.add(Floats.toArray(vectorAsList));
                }
            }
            
            if (vectorsInBranch.isEmpty()) {
                continue;
            }

            // Berechne den initialen Sonden-Vektor (Centroid der Säule).
            float[] probeVector = calculateCentroid(vectorsInBranch);

            // Steuere den Sonden-Vektor mit dem User-Vektor, um die Suche zu verfeinern.
            float[] steeredProbeVector = steerVector(probeVector, userVector, MagicNumbers.SERENDIPITY_ALHPA.asDouble());

         // Feuere die Sonde ab, um den Gewinner-Cluster zu finden.
            List<QueryResult> probeResults = databaseQuery.search(
                MagicNumbers.QDRANT_CENTROID_COLLECTION_NAME.asString(), steeredProbeVector, 1);

            if (!probeResults.isEmpty()) {
                String winnerId = (String) probeResults.get(0).payload().get("id");

                // NEUER SCHRITT: "Ground Truth"-Suche innerhalb des Gewinner-Clusters.
                List<QueryResult> bestPaperResult = databaseQuery.searchWithFilter(
                    MagicNumbers.QDRANT_VECTOR_COLLECTION_NAME.asString(), // Suche in der Paper-Collection!
                    userVector,                                           // Mit dem originalen User-Vektor!
                    1,                                                    // Nur das beste Ergebnis!
                    winnerId                                              // Filter auf den Gewinner-Cluster!
                );

                if (!bestPaperResult.isEmpty()) {
                    // Extrahiere den Score und speichere ihn.
                    float bestPaperScore = bestPaperResult.get(0).score();
                    winnerMap.put(winnerId, bestPaperScore);
                }
            }
        }
        
        return winnerMap;
    }

    /**
     * Steuert einen Vektor in Richtung eines anderen Vektors mittels linearer Interpolation.
     * @param original Der ursprüngliche Vektor (z.B. der Centroid der Säule).
     * @param target Der Zielvektor (z.B. der Vektor der Nutzeranfrage).
     * @param alpha Der Steuerungsfaktor (0.0 = keine Änderung, 1.0 = komplett Zielvektor).
     * @return Der neue, gesteuerte Vektor.
     */
    private static float[] steerVector(float[] original, float[] target, double alpha) {
        if (alpha <= 0.0 || target == null || original.length != target.length) return original;
        if (alpha >= 1.0) return target;
        
        int dimensions = original.length;
        float[] steered = new float[dimensions];
        float alphaFloat = (float) alpha;

        for (int i = 0; i < dimensions; i++) {
            steered[i] = (1 - alphaFloat) * original[i] + alphaFloat * target[i];
        }
        return steered;
    }
    
    /**
     * Berechnet den durchschnittlichen Vektor (Centroid) aus einer Liste von Vektoren.
     * @param vectors Die Liste der Vektoren.
     * @return Der resultierende Centroid-Vektor.
     */
    private static float[] calculateCentroid(List<float[]> vectors) {
        if (vectors == null || vectors.isEmpty()) return new float[0];
        int dimensions = vectors.get(0).length;
        float[] sumVector = new float[dimensions];
        
        for (float[] vector : vectors) {
            for (int i = 0; i < dimensions; i++) {
                sumVector[i] += vector[i];
            }
        }
        
        for (int i = 0; i < dimensions; i++) {
            sumVector[i] /= vectors.size();
        }
        
        return sumVector;
    }

    /**
     * Findet den kleinsten (tiefsten) gemeinsamen Vorfahren-Cluster für eine Liste von Cluster-IDs.
     * @param clusterIds Eine Liste von Cluster-IDs (z.B. "all_vectors-0-1").
     * @return Ein Optional, das die ID des gemeinsamen Vorfahren enthält, oder leer ist.
     */
    private static Optional<String> findSmallestCommonCluster(List<String> clusterIds) {
        if (clusterIds == null || clusterIds.isEmpty()) return Optional.empty();
        
        // Start mit dem Pfad des ersten Clusters als potenziell gemeinsamer Pfad.
        String[] commonPrefix = clusterIds.get(0).split("-");

        // Vergleiche diesen Pfad mit allen anderen Pfaden.
        for (int i = 1; i < clusterIds.size(); i++) {
            String[] currentPath = clusterIds.get(i).split("-");
            int minLength = Math.min(commonPrefix.length, currentPath.length);
            int prefixLength = 0;
            
            // Finde heraus, wie viele Segmente von Anfang an übereinstimmen.
            for (int j = 0; j < minLength; j++) {
                if (commonPrefix[j].equals(currentPath[j])) {
                    prefixLength++;
                } else {
                    break; 
                }
            }
            
            // Kürze den gemeinsamen Pfad auf die Länge der Übereinstimmung.
            if (prefixLength < commonPrefix.length) {
                commonPrefix = Arrays.copyOf(commonPrefix, prefixLength);
            }
        }
        
        // Baue die finale ID aus dem gemeinsamen Präfix zusammen.
        return (commonPrefix.length == 0) ? Optional.empty() : Optional.of(String.join("-", commonPrefix));
    }
}