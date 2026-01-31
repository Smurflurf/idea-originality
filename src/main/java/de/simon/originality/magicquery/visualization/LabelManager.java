package de.simon.originality.magicquery.visualization;

import java.awt.FontMetrics;
import java.awt.Graphics2D;
import java.awt.geom.Rectangle2D;
import java.awt.image.BufferedImage;
import java.util.ArrayList;
import java.util.List;

public class LabelManager {
	private final List<Label> labels = new ArrayList<>();
	private final BufferedImage image;
	private final Graphics2D g2d;

	public LabelManager(BufferedImage image) {
		this.image = image;
		this.g2d = image.createGraphics();
	}

	/**
	 * Adds a new Label and calculates its bounding box.
	 */
	public void addLabel(Label label) {
		updateBoundingBox(label);
		this.labels.add(label);
	}

	/**
	 * Claculates a Labels bounding box based on its current position and its font.
	 */
    private void updateBoundingBox(Label label) {
        g2d.setFont(label.font);
        FontMetrics fm = g2d.getFontMetrics();
        Rectangle2D bounds = fm.getStringBounds(label.text, g2d);
        
        int boxWidth = (int) bounds.getWidth();
        int boxHeight = (int) bounds.getHeight();
        
        int boxX = label.x - boxWidth / 2;
        int boxY = label.y - boxHeight / 2;

        label.boundingBox.setBounds(boxX, boxY, boxWidth, boxHeight);
    }

	/**
	 * Iteratively un-overlapps the Labels.
	 */
	public void adjustLabels(int maxIterations, double initialShiftAmount) {
//		System.out.println("Beginne mit der Anpassung der Label-Positionen...");

		// "Cooling"-Faktor: Die Verschiebungen werden mit jeder Iteration kleiner
		double shiftAmount = initialShiftAmount;

		for (int i = 0; i < maxIterations; i++) {
			int collisions = 0;

			// Phase 1: Kollisionen erkennen und Labels voneinander wegstoßen
			for (int j = 0; j < labels.size(); j++) {
				for (int k = j + 1; k < labels.size(); k++) {
					Label label1 = labels.get(j);
					Label label2 = labels.get(k);

					if (label1.boundingBox.intersects(label2.boundingBox)) {
						collisions++;

						// KORREKTUR: Direkte Abstoßung statt radialer Verschiebung
						double dx = label2.x - label1.x;
						double dy = label2.y - label1.y;
						double distance = Math.sqrt(dx * dx + dy * dy);

						if (distance > 1e-6) {
							double move = shiftAmount / 2.0;
							double moveX = (dx / distance) * move;
							double moveY = (dy / distance) * move;

							// Stoße beide Labels in entgegengesetzte Richtungen
							label1.x -= moveX;
							label1.y -= moveY;
							label2.x += moveX;
							label2.y += moveY;
						}
					}
				}
			}

			// Phase 2: Randprüfung und Aktualisierung der Bounding-Boxen für alle Labels
			for (Label label : labels) {
				// KORREKTUR: Strikte Randprüfung für die Bounding-Box
				int halfWidth = label.boundingBox.width / 2;
				int halfHeight = label.boundingBox.height / 2;

				// Klemmt den Mittelpunkt so ein, dass die Box nie den Rand verlässt
				label.x = Math.max(halfWidth, Math.min(image.getWidth() - halfWidth, label.x));
				label.y = Math.max(halfHeight, Math.min(image.getHeight() - halfHeight, label.y));

				// WICHTIG: Bounding-Box für die nächste Iteration aktualisieren
				updateBoundingBox(label);
			}

			if (collisions == 0) {
//				System.out.println("Anpassung nach " + (i + 1) + " Iterationen abgeschlossen.");
				break;
			}

			// Reduziere die Verschiebungsstärke für die nächste Iteration ("Abkühlen")
			shiftAmount *= 0.9;


			if (i == maxIterations - 1) {
//				System.out.println("Anpassung nach " + maxIterations + " Iterationen abgeschlossen. Evtl. noch Überlappungen.");
			}
		}
		g2d.dispose();
	}

	public List<Label> getFinalLabels() {
		return this.labels;
	}
}