package de.simon.originality.magicquery.visualization;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.awt.image.ConvolveOp;
import java.awt.image.Kernel;

/**
 * Eine Utility-Klasse für Bildmanipulation, die auf den zentralen
 * ImageCacheService zugreift, um Bilder aus dem RAM zu holen.
 */
public final class ImageService {
    /**
     * Erstellt ein neues Bild, indem es nicht-transparente rote Pixel einfärbt.
     * Behält die ursprüngliche Transparenz bei.
     */
    public static BufferedImage colorize(BufferedImage stencil, Color newColor) {
        if (stencil == null) return null;
        int width = stencil.getWidth();
        int height = stencil.getHeight();
        BufferedImage colorizedImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);

        int newRgb = newColor.getRGB() & 0x00FFFFFF;

        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                int originalRgb = stencil.getRGB(x, y);
                int originalAlpha = (originalRgb >> 24) & 0xff;
                int originalRed = (originalRgb >> 16) & 0xff;

                if (originalAlpha > 0 && originalRed > 128) {
                    int finalRgb = (originalAlpha << 24) | newRgb;
                    colorizedImage.setRGB(x, y, finalRgb);
                }
            }
        }
        return colorizedImage;
    }

    public static BufferedImage applyBlur(BufferedImage source) {
        if (source == null) return null;
        float[] matrix = new float[9];
        for (int i = 0; i < 9; i++) {
            matrix[i] = 1.0f / 9.0f;
        }
        Kernel kernel = new Kernel(3, 3, matrix);
        ConvolveOp op = new ConvolveOp(kernel, ConvolveOp.EDGE_NO_OP, null);
        return op.filter(source, null);
    }
}