package de.simon.originality.dto;

import io.swagger.v3.oas.annotations.media.Schema;

public record ApiSearchRequestDto(
	@Schema(
			description = "The scientific query or idea summary to be analyzed. " +
					"Please follow the detailed formatting guidelines provided in the endpoint description.", 
			example = "Quantum computing algorithms for optimizing supply chain logistics in global trade."
    )
    String query
) {}