package de.simon.originality.security;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class RateLimitingService {

    // Speichert den "Eimer" (Bucket) für jede IP-Adresse im RAM
    private final Map<String, Bucket> cache = new ConcurrentHashMap<>();

    public Bucket resolveBucket(String ip) {
        return cache.computeIfAbsent(ip, this::newBucket);
    }

    private Bucket newBucket(String ip) {
        // Regel: Fülle 1 Token pro 1 Sekunde auf.
        // Kapazität: Maximal 2 Tokens (Erlaubt einen winzigen "Burst" von 2 Anfragen gleichzeitig, 
        // falls das Netzwerk kurz ruckelt, blockiert aber danach strikt auf 1 Anfrage/Sekunde).
        Bandwidth limit = Bandwidth.builder()
                .capacity(2) 
                .refillIntervally(1, Duration.ofSeconds(1))
                .build();
        
        return Bucket.builder().addLimit(limit).build();
    }
}