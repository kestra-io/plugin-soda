package io.kestra.plugin.soda.models;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

@Value
@Jacksonized
@SuperBuilder
@JsonIgnoreProperties(ignoreUnknown = true)
public class ContractVerificationResult {
    @Schema(title = "Result of every contract verified in this session.", description = "One entry per file passed via `contracts`. A partial failure (one contract errors while another passes) still produces one entry per contract here.")
    List<ContractResult> contractResults;

    @Schema(title = "True if at least one check across all contracts failed.")
    Boolean hasFailures;

    @Schema(title = "True if at least one check across all contracts produced a warning.")
    Boolean hasWarnings;

    @Schema(title = "True if verifying any contract raised an error independent of any check outcome.")
    Boolean hasErrors;
}
