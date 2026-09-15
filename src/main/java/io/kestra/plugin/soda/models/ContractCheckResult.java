package io.kestra.plugin.soda.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

@Value
@Jacksonized
@SuperBuilder
@JsonIgnoreProperties(ignoreUnknown = true)
public class ContractCheckResult {
    @Schema(title = "Check name as defined in the contract's `checks` list, or a generated identity when the check has no explicit name.")
    String name;

    @Schema(title = "Check type, e.g. `row_count`, `missing`, `duplicate`, as defined in the contract.")
    String type;

    @Schema(title = "Column the check applies to, when the check is column-scoped.")
    String column;

    @Schema(title = "Human-readable definition of the check, when available.")
    String definition;

    @Schema(title = "Outcome of the check.", description = "One of `passed`, `failed`, `warn`, `not_evaluated`, `excluded`.")
    ContractCheckOutcome outcome;
}
