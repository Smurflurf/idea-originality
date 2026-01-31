package de.simon.originality.magicquery.extract_idea;

/**
 * Describes a file by its name, type and content bytes.
 */
public record FileData(
		String originalFilename, 
		String contentType, 
		byte[] content
		) {}