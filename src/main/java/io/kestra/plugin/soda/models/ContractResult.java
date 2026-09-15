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
public class ContractResult {
    @Schema(title = "Name of the data source the contract was verified against.")
    String dataSource;

    @Schema(title = "Dataset the contract targets, as defined by the contract's `dataset` field.")
    String dataset;

    @Schema(title = "Outcome of every check declared in the contract.")
    List<ContractCheckResult> checks;

    @Schema(title = "True if at least one check for this contract failed.")
    Boolean hasFailures;

    @Schema(title = "True if at least one check for this contract produced a warning.")
    Boolean hasWarnings;

    @Schema(title = "True if verifying this contract raised an error independent of any check outcome (e.g. a connection or parsing failure).")
    Boolean hasErrors;
}
