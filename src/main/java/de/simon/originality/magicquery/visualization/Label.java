package de.simon.originality.magicquery.visualization;

import java.awt.Color;
import java.awt.Font;
import java.awt.Rectangle;

/**
 * A data class keeping all information for a label; also its changeable position and bounding box.
 */
public class Label {
    public final String text;
    public final Font font;
    public final Color color;
    public final Color outlineColor;

    public int x; 
    public int y;
    public Rectangle boundingBox;

    public Label(String text, int x, int y, Font font, Color color, Color outlineColor) {
        this.text = text;
        this.x = x;
        this.y = y;
        this.font = font;
        this.color = color;
        this.outlineColor = outlineColor;
        this.boundingBox = new Rectangle();
    }
}