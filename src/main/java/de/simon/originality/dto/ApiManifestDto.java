package de.simon.originality.dto;

public record ApiManifestDto(
    String apiName, 
    String description, 
    String version, 
    String openapiSpecUrl
) {}