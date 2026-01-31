package de.simon.originality.magicquery.visualization;

import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import javax.imageio.ImageIO;

import org.springframework.stereotype.Service;

import de.simon.originality.magicquery.DataPathService;
import de.simon.originality.magicquery.MagicNumbers;
import jakarta.annotation.PostConstruct;


@Service
public class ImageCacheService {
    public static record CachedImage(BufferedImage image, int originalWidth) {}
    private static final Map<String, byte[]> cache = new ConcurrentHashMap<>();
    
    @PostConstruct
    public void preloadImages() {
        System.out.println("--- Starte Pre-Loading der komprimierten PNG-Rohdaten in den RAM-Cache ---");
        Path imagesPath = DataPathService.getImagesPath();
        long startTime = System.currentTimeMillis();

        try (Stream<Path> paths = Files.walk(imagesPath)) {
            paths
                .filter(Files::isRegularFile)
                .filter(path -> path.toString().endsWith(".png"))
                .parallel()
                .forEach(path -> {
                    try {
                        byte[] imageBytes = Files.readAllBytes(path);
                        String key = path.getFileName().toString().replace(".png", "");
                        cache.put(key, imageBytes);
                    } catch (IOException e) {
                        System.err.println("FEHLER beim Laden der Bild-Bytes in den Cache: " + path + " - " + e.getMessage());
                    }
                });
        } catch (IOException e) {
            System.err.println("FATALER FEHLER: Konnte das Bilderverzeichnis nicht durchsuchen: " + imagesPath);
        }

        long endTime = System.currentTimeMillis();
        System.out.println(String.format("-> Pre-Loading abgeschlossen. %d Bilder in %.2f Sekunden als Rohdaten in den Cache geladen.",
            cache.size(), (endTime - startTime) / 1000.0));
    }

    /**
     * Loads a scaled down image
     * @param key
     * @return
     * @throws IOException
     */
    public static CachedImage getCachedImageWithInfo(String key) throws IOException {
        byte[] imageBytes = cache.get(key);
        if (imageBytes == null) {
            return null;
        }

        BufferedImage originalImage = ImageIO.read(new ByteArrayInputStream(imageBytes));
        
        if (originalImage == null) {
            return null;
        }
        
        int originalWidth = originalImage.getWidth();

        if (originalWidth > MagicNumbers.MAX_IMAGE_WIDTH.asInteger()) {
            int newWidth = MagicNumbers.MAX_IMAGE_WIDTH.asInteger();
            int newHeight = (newWidth * originalImage.getHeight()) / originalWidth;

            BufferedImage scaledImage = new BufferedImage(newWidth, newHeight, BufferedImage.TYPE_INT_ARGB);
            Graphics2D g2d = scaledImage.createGraphics();
            g2d.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BILINEAR);
            g2d.drawImage(originalImage, 0, 0, newWidth, newHeight, null);
            g2d.dispose();
            
            return new CachedImage(scaledImage, originalWidth);
        } else {
            return new CachedImage(originalImage, originalWidth);
        }
    }
}