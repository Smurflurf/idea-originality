package de.simon.originality.magicquery.visualization;

public record LabelData(
		String clusterId,
		String text,
		double x_data, 
		double y_data, 
		String color,
		int fontSize_base, 
		String fontFamily,
		String fontWeight
		) {}