package io.kestra.plugin.soda.models;

import com.fasterxml.jackson.annotation.JsonEnumDefaultValue;
import com.fasterxml.jackson.annotation.JsonFormat;

/**
 * Mirrors soda-core 4's {@code CheckOutcome} enum values (lowercased) verbatim, verified against
 * soda-core 4.23.1: {@code PASSED}, {@code FAILED}, {@code WARN}, {@code NOT_EVALUATED},
 * {@code EXCLUDED}. {@code READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE} maps any outcome value a
 * future soda-core build might add to {@link #unknown} instead of failing the whole
 * {@code result.json} deserialization.
 */
@JsonFormat(with = JsonFormat.Feature.READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE)
public enum ContractCheckOutcome {
    passed,
    failed,
    warn,
    not_evaluated,
    excluded,
    @JsonEnumDefaultValue
    unknown,
}
