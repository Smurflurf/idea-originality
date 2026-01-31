package de.simon.originality.magicquery.visualization;

import java.awt.Color;
import java.awt.FontMetrics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.geom.Rectangle2D;
import java.awt.image.BufferedImage;
import java.util.List;

public class LabelDrawer {

    private final Graphics2D g2d;
    
    public LabelDrawer(BufferedImage image) {
        this.g2d = image.createGraphics();
        g2d.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON);
        g2d.setRenderingHint(RenderingHints.KEY_FRACTIONALMETRICS, RenderingHints.VALUE_FRACTIONALMETRICS_ON);
    }

    /**
     * Draws a List of already positioned Labels.
     */
    public void drawFinalLabels(List<Label> labels) {
        for (Label label : labels) {
            g2d.setFont(label.font);
            
            FontMetrics fm = g2d.getFontMetrics();
            Rectangle2D bounds = fm.getStringBounds(label.text, g2d);

            int drawX = (int) (label.x - bounds.getWidth() / 2);
            int drawY = (int) (label.y - bounds.getHeight() / 2 + fm.getAscent());

            drawHighContrastText(label.text, drawX, drawY, label.color, 4, 2);
        }
        g2d.dispose();
    }

    /**
     * Draws a String with two outlines, a black and white one.
     */
    private void drawHighContrastText(String text, int x, int y, Color textColor, int outerStrokeWidth, int innerStrokeWidth) {
        g2d.setColor(Color.WHITE);
        for (int dx = -outerStrokeWidth; dx <= outerStrokeWidth; dx++) {
            for (int dy = -outerStrokeWidth; dy <= outerStrokeWidth; dy++) {
                if (dx * dx + dy * dy <= outerStrokeWidth * outerStrokeWidth) {
                     g2d.drawString(text, x + dx, y + dy);
                }
            }
        }
        
        g2d.setColor(Color.BLACK);
        for (int dx = -innerStrokeWidth; dx <= innerStrokeWidth; dx++) {
            for (int dy = -innerStrokeWidth; dy <= innerStrokeWidth; dy++) {
                 if (dx * dx + dy * dy <= innerStrokeWidth * innerStrokeWidth) {
                     g2d.drawString(text, x + dx, y + dy);
                }
            }
        }
        
        g2d.setColor(textColor);
        g2d.drawString(text, x, y);
    }
}