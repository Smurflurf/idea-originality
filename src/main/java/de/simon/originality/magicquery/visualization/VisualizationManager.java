package de.simon.originality.magicquery.visualization;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.Stroke;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import de.simon.originality.magicquery.MagicNumbers;
import de.simon.originality.magicquery.cluster_analysis.graph.GraphNode;
import de.simon.originality.magicquery.cluster_analysis.graph.KnowledgeGraphService;
import de.simon.originality.magicquery.database_interact.DatabaseQuery.QueryResult;

/**
 * Manages the creation of a modular, layered visualization.
 * It generates separate, transparent image layers for points, outlines, labels, and neighbors,
 * allowing for interactive toggling on the frontend.
 */
public class VisualizationManager {
    private final int originalBaseImageWidth;
	private final BufferedImage baseImage;
	private final int width;
	private final int height;
    private final Map<String, ImageCacheService.CachedImage> requestScopedImageCache = new HashMap<>();
    
	public VisualizationManager() throws IOException {
        ImageCacheService.CachedImage baseImageData = getCachedOrDecodedImage(MagicNumbers.ROOT_CLUSTER_NAME.asString());
        if (baseImageData == null || baseImageData.image() == null) {
            throw new IOException("Base image 'all_vectors.png' konnte nicht aus dem Cache geladen werden.");
        }
        this.baseImage = baseImageData.image();
        this.originalBaseImageWidth = baseImageData.originalWidth();
        this.width = baseImage.getWidth();
        this.height = baseImage.getHeight();
	}


	public BufferedImage getBaseImage() {
		return this.baseImage;
	}

	public double getAspectRatio() {
		return (this.width > 0) ? (double) this.height / this.width : 1.0;
	}

	public Map<String, String> getHexColorMap(Set<String> clusterIdsToColor) {
		List<VisualizationLayer> layers = createSortedLayers(clusterIdsToColor);
		Map<String, Color> colorMap = generateColorPalette(layers);
		return colorMap.entrySet().stream()
				.collect(Collectors.toMap(
						Map.Entry::getKey,
						entry -> String.format("#%02x%02x%02x",
								entry.getValue().getRed(),
								entry.getValue().getGreen(),
								entry.getValue().getBlue())
						));
	}

	public BufferedImage drawPointsLayer(Set<String> clusterIdsToColor) throws IOException {
		List<VisualizationLayer> layers = createSortedLayers(clusterIdsToColor);
		Map<String, Color> colorMap = generateColorPalette(layers);

		BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
		Graphics2D g2d = image.createGraphics();

		for (VisualizationLayer layer : layers) {
			String id = layer.getClusterId();
			Color color = colorMap.getOrDefault(id, Color.WHITE);

            ImageCacheService.CachedImage noiseStencilData = getCachedOrDecodedImage(id + "-noise");
            if (noiseStencilData != null && noiseStencilData.image() != null) {
                g2d.drawImage(noiseStencilData.image(), 0, 0, null);
            }

            ImageCacheService.CachedImage pointStencilData = getCachedOrDecodedImage(id);
            if (pointStencilData != null && pointStencilData.image() != null) {
                 g2d.drawImage(ImageService.colorize(pointStencilData.image(), color), 0, 0, null);
            }

		}
		g2d.dispose();
		return image;
	}

	public BufferedImage drawOutlinesLayer(Set<String> clusterIdsToColor) throws IOException {
        List<VisualizationLayer> layers = createSortedLayers(clusterIdsToColor);
        Map<String, Color> colorMap = generateColorPalette(layers);

        BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
        Graphics2D g2d = image.createGraphics();

        List<VisualizationLayer> reversedLayers = new ArrayList<>(layers);
        Collections.reverse(reversedLayers);
        
        for (VisualizationLayer layer : reversedLayers) {
            String id = layer.getClusterId();
            Color color = colorMap.getOrDefault(id, Color.WHITE);
            
            ImageCacheService.CachedImage outlineStencilData = getCachedOrDecodedImage(id + "-outline");
            
            if (outlineStencilData != null && outlineStencilData.image() != null) {
                Color outlineColor = (layer.getLevel() <= 1) ? color.darker() : color;
                g2d.drawImage(ImageService.colorize(outlineStencilData.image(), outlineColor), 0, 0, null);
            }
        }
        g2d.dispose();
        return image;
    }
	
	/*public BufferedImage drawLabelsLayer(Set<String> clusterIdsToColor) {
		List<VisualizationLayer> layers = createSortedLayers(clusterIdsToColor);
		Map<String, Color> colorMap = generateColorPalette(layers);

		BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
        double fontScale = (double) this.width / this.originalBaseImageWidth;
		LabelManager labelManager = new LabelManager(image);

		for (VisualizationLayer layer : layers) {
			Optional<GraphNode> nodeOpt = KnowledgeGraphService.getNodeAttributes(layer.getClusterId());
			if (nodeOpt.isPresent()) {
				GraphNode node = nodeOpt.get();
				if (node.getClusterName() != null && !node.getClusterName().isBlank() && node.getPos2d() != null) {
					int[] pixelCoords = transformDataToPixel(node.getPos2d().get(0), node.getPos2d().get(1));

					int baseFontSize = Math.max(12, Math.min((int) (12 + 20 * Math.log1p(node.getSize() / 1000.0)), 54));
					int scaledFontSize = (int) (baseFontSize * fontScale * MagicNumbers.FONT_SCALE_FACTOR.asDouble());
	                Font font = new Font("Roboto", Font.BOLD, scaledFontSize);


					Color labelColor = colorMap.getOrDefault(layer.getClusterId(), Color.BLACK).brighter();
					Label label = new Label(node.getClusterName(), pixelCoords[0], pixelCoords[1], font, labelColor, Color.LIGHT_GRAY);

					labelManager.addLabel(label);
				}
			}
		}
		labelManager.adjustLabels(100, 10.0);
		new LabelDrawer(image).drawFinalLabels(labelManager.getFinalLabels());
		return image;
	}*/

	/*public BufferedImage drawContextLayer(Set<String> userClusterIds) throws IOException {
        BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
        Set<String> finalContextClusterIds = new HashSet<>();

        List<String> topLevelClusters = KnowledgeGraphService.getChildren(MagicNumbers.ROOT_CLUSTER_NAME.asString());
        for (String topLevelId : topLevelClusters) {
            findSignificantClusters(topLevelId, finalContextClusterIds, true);
        }

        if (userClusterIds != null) {
            finalContextClusterIds.removeAll(userClusterIds);
        }

        Graphics2D g2d = image.createGraphics();
        g2d.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        
        double fontScale = (double) this.width / this.originalBaseImageWidth;
        
        LabelManager labelManager = new LabelManager(image);
        Color contextColor = new Color(128, 128, 128, 100);

        for (String contextId : finalContextClusterIds) {
            ImageCacheService.CachedImage outlineStencilData = getCachedOrDecodedImage(contextId + "-outline");
            if (outlineStencilData != null && outlineStencilData.image() != null) {
                g2d.drawImage(ImageService.colorize(outlineStencilData.image(), contextColor), 0, 0, null);
            }

            KnowledgeGraphService.getNodeAttributes(contextId).ifPresent(node -> {
                if (node.getClusterName() != null && !node.getClusterName().isBlank() && node.getPos2d() != null) {
                    int[] pixelCoords = transformDataToPixel(node.getPos2d().get(0), node.getPos2d().get(1));
                    
                    int baseFontSize = Math.max(12, Math.min((int) (12 + 20 * Math.log1p(node.getSize() / 1000.0)), 54));
                    int scaledFontSize = (int) (baseFontSize * fontScale * MagicNumbers.FONT_SCALE_FACTOR.asDouble());
                    Font font = new Font("Roboto", Font.BOLD, scaledFontSize);
                    Label label = new Label(node.getClusterName(), pixelCoords[0], pixelCoords[1], font, contextColor.brighter(), Color.BLACK);
                    labelManager.addLabel(label);
                }
            });
        }
        labelManager.adjustLabels(100, 10.0);
        new LabelDrawer(image).drawFinalLabels(labelManager.getFinalLabels());
        g2d.dispose();
        return image;
    }*/

	/**
	 * Generiert eine Liste von LabelData-Objekten für das Frontend.
	 * Enthält alle notwendigen Informationen, um die Labels clientseitig mit SVG/HTML zu rendern.
	 * Die Kollisionsvermeidung wird bewusst weggelassen und dem Frontend überlassen.
	 */
	public List<LabelData> generateLabelData(Set<String> clusterIdsToLabel, Map<String, Color> colorMap) {
		List<LabelData> labelDataList = new ArrayList<>();
		double fontScale = (double) MagicNumbers.MAX_IMAGE_WIDTH.asInteger() / this.originalBaseImageWidth;

		for (String clusterId : clusterIdsToLabel) {
			KnowledgeGraphService.getNodeAttributes(clusterId).ifPresent(node -> {
				if (node.getClusterName() != null && !node.getClusterName().isBlank() && node.getPos2d() != null) {
					double dataX = node.getPos2d().get(0);
					double dataY = node.getPos2d().get(1);

					int baseFontSize = Math.max(12, Math.min((int) (12 + 20 * Math.log1p(node.getSize() / 1000.0)), 54));
					int scaledBaseFontSize = (int) (baseFontSize * fontScale * MagicNumbers.FONT_SCALE_FACTOR.asDouble());

					Color awtColor = colorMap.getOrDefault(clusterId, Color.BLACK).brighter();
					String hexColor = String.format("#%02x%02x%02x", awtColor.getRed(), awtColor.getGreen(), awtColor.getBlue());

					labelDataList.add(new LabelData(
							node.getId(),
							node.getClusterName(),
							dataX, 
							dataY, 
							hexColor,
							scaledBaseFontSize, 
							"Roboto",
							"bold"
							));
				}
			});
		}
		return labelDataList;
	}

	/**
     * Extrahiert die Logik zum Finden der Kontext-Cluster, damit sie im Controller wiederverwendet werden kann.
     */
    public Set<String> findContextClusterIds() {
        Set<String> contextClusterIds = new HashSet<>();
        List<String> topLevelClusters = KnowledgeGraphService.getChildren(MagicNumbers.ROOT_CLUSTER_NAME.asString());
        for (String topLevelId : topLevelClusters) {
            findSignificantClusters(topLevelId, contextClusterIds, true);
        }
        return contextClusterIds;
    }

	public BufferedImage drawContextLayer(Set<String> userClusterIds) throws IOException {
        BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
        
        Set<String> finalContextClusterIds = findContextClusterIds();
        if (userClusterIds != null) {
            finalContextClusterIds.removeAll(userClusterIds);
        }

        Graphics2D g2d = image.createGraphics();
        g2d.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        
        Color contextColor = new Color(128, 128, 128, 100);

        for (String contextId : finalContextClusterIds) {
            ImageCacheService.CachedImage outlineStencilData = getCachedOrDecodedImage(contextId + "-outline");
            if (outlineStencilData != null && outlineStencilData.image() != null) {
                g2d.drawImage(ImageService.colorize(outlineStencilData.image(), contextColor), 0, 0, null);
            }
        }
        g2d.dispose();
        return image;
    }

	public BufferedImage drawNeighborsLayer(List<QueryResult> neighbors) {
		BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
		Graphics2D g2d = image.createGraphics();
		g2d.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);

		if (neighbors != null && !neighbors.isEmpty()) {
			for (QueryResult neighbor : neighbors) {
				String finalClusterId = getFinalClusterId(neighbor);
				if (finalClusterId != null) {
					KnowledgeGraphService.getClusterPosition(finalClusterId).ifPresent(pos2d -> {
						int[] pixelCoords = transformDataToPixel(pos2d.get(0), pos2d.get(1));
						g2d.setColor(new Color(255, 230, 150, 220)); 
						g2d.fillOval(pixelCoords[0] - 5, pixelCoords[1] - 5, 10, 10);
						g2d.setColor(new Color(0, 0, 0, 220));
						g2d.setStroke(new BasicStroke(1.5f));
						g2d.drawOval(pixelCoords[0] - 5, pixelCoords[1] - 5, 10, 10);
					});
				}
			}
		}
		g2d.dispose();
		return image;
	}

	public BufferedImage drawCrosshairLayer(float[] crossPosition) {
	    BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
	    if (crossPosition != null && crossPosition.length == 2) {
	        Graphics2D g2d = image.createGraphics();
	        int[] pixelCoords = transformDataToPixel(crossPosition[0], crossPosition[1]);

	        int clampedX = Math.max(0, Math.min(width - 1, pixelCoords[0]));
	        int clampedY = Math.max(0, Math.min(height - 1, pixelCoords[1]));

	        g2d.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
	        g2d.setColor(new Color(255, 255, 0, 200));
	        Stroke dashed = new BasicStroke(2, BasicStroke.CAP_BUTT, BasicStroke.JOIN_BEVEL, 0, new float[]{9}, 0);
	        g2d.setStroke(dashed);

	        g2d.drawLine(0, clampedY, width, clampedY);
	        g2d.drawLine(clampedX, 0, clampedX, height); 

	        g2d.dispose();
	    }
	    return image;
	}

	/**
	 * Recursively explores the cluster hierarchy to find significant clusters for the context view.
	 * It dives into branches where parent clusters exceed 1 million entries and collects their children that have more than 250,000 entries.
	 * @param clusterId           The ID of the cluster to start the search from.
	 * @param significantClusters A Set to accumulate the IDs of clusters to be displayed.
	 * @param isTopLevel          A flag to bypass the 250k filter for the initial top-level clusters.
	 */
	private void findSignificantClusters(String clusterId, Set<String> significantClusters, boolean isTopLevel) {
		Optional<GraphNode> nodeOpt = KnowledgeGraphService.getNodeAttributes(clusterId);
		if (nodeOpt.isEmpty()) {
			return;
		}

		GraphNode node = nodeOpt.get();

		if (node.getSize() > MagicNumbers.MINIMUM_PARENT_CLUSTER_SIZE.asInteger()) {
			List<String> children = KnowledgeGraphService.getChildren(clusterId);
			for (String childId : children) {
				findSignificantClusters(childId, significantClusters, false);
			}
		}
		else {
			Optional<String> parentIdOpt = KnowledgeGraphService.getParent(clusterId);
			if (parentIdOpt.isPresent()) {
				Optional<GraphNode> parentNodeOpt = KnowledgeGraphService.getNodeAttributes(parentIdOpt.get());
				if (parentNodeOpt.isPresent() && parentNodeOpt.get().getSize() > MagicNumbers.MINIMUM_PARENT_CLUSTER_SIZE.asInteger()) {
					if (isTopLevel || node.getSize() > MagicNumbers.MINIMUM_SHOWABLE_CLUSTER_SIZE.asInteger()) {
						significantClusters.add(clusterId);
					}
				} else {
					significantClusters.add(clusterId);
				}
			} else {
				significantClusters.add(clusterId);
			}
		}
	}    

	@SuppressWarnings("unchecked")
	private String getFinalClusterId(QueryResult neighbor) {
		Object hierarchyObj = neighbor.payload().get("cluster_hierarchy");
		if (hierarchyObj instanceof List) {
			List<String> hierarchy = (List<String>) hierarchyObj;
			if (!hierarchy.isEmpty()) {
				return hierarchy.get(hierarchy.size() - 1);
			}
		}
		Object idObj = neighbor.payload().get("id");
		if (idObj instanceof String) {
			return (String) idObj;
		}
		return null;
	}

	/**
	 * Helper to create a sorted list of VisualizationLayer objects.
	 */
	private List<VisualizationLayer> createSortedLayers(Set<String> clusterIds) {
		List<VisualizationLayer> layers = new ArrayList<>();
		for (String id : clusterIds) {
			KnowledgeGraphService.getClusterPosition(id).ifPresent(pos -> {
				int level = id.split("-").length;
				layers.add(new VisualizationLayer(id, level, pos));
			});
		}
		Collections.sort(layers); // Sorts by level, ensuring parents are processed first
		return layers;
	}

	/**
	 * Generates a palette of visually distinct colors for the given layers.
	 */
	public Map<String, Color> generateColorPalette(List<VisualizationLayer> layers) {
		Map<String, Color> colorMap = new HashMap<>();
		List<String> clusterIds = layers.stream().map(VisualizationLayer::getClusterId).collect(Collectors.toList());
		int numberOfColors = clusterIds.size();
		if (numberOfColors == 0) return colorMap;

		float[][] sbProfiles = {{0.95f, 0.98f}, {0.80f, 1.0f}, {1.0f, 0.9f}};
		float hueStep = 1.0f / numberOfColors;
		float currentHue = 0.0f;

		for (int i = 0; i < numberOfColors; i++) {
			float[] profile = sbProfiles[i % sbProfiles.length];
			Color color = Color.getHSBColor(currentHue, profile[0], profile[1]);
			colorMap.put(clusterIds.get(i), color);
			currentHue += hueStep;
		}
		return colorMap;
	}

	/**
	 * Converts UMAP data coordinates (from knowledge graph) to image pixel coordinates.
	 */
	public int[] transformDataToPixel(double dataX, double dataY) {
		double dataWidth = KnowledgeGraphService.getXMax() - KnowledgeGraphService.getXMin();
		double dataHeight = KnowledgeGraphService.getYMax() - KnowledgeGraphService.getYMin();
		if (dataWidth == 0 || dataHeight == 0) {
			return new int[]{this.width / 2, this.height / 2};
		}
		double relativeX = (dataX - KnowledgeGraphService.getXMin()) / dataWidth;
		double relativeY = 1.0 - ((dataY - KnowledgeGraphService.getYMin()) / dataHeight);
		return new int[]{(int) (relativeX * this.width), (int) (relativeY * this.height)};

	}
	
    private ImageCacheService.CachedImage getCachedOrDecodedImage(String key) throws IOException {
        String cleanKey = key.replace(".png", "");
        if (requestScopedImageCache.containsKey(cleanKey)) {
            return requestScopedImageCache.get(cleanKey);
        }
        ImageCacheService.CachedImage imageInfo = ImageCacheService.getCachedImageWithInfo(cleanKey);
        requestScopedImageCache.put(cleanKey, imageInfo);
        return imageInfo;
    }


}