package io.kestra.plugin.soda.models;

/**
 * Mirrors soda-core 4's {@code CheckOutcome} enum values (lowercased) verbatim, verified against
 * soda-core 4.23.1: {@code PASSED}, {@code FAILED}, {@code WARN}, {@code NOT_EVALUATED},
 * {@code EXCLUDED}.
 */
public enum ContractCheckOutcome {
    passed,
    failed,
    warn,
    not_evaluated,
    excluded,
}
