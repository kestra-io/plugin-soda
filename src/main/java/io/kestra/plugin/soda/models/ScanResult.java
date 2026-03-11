package io.kestra.plugin.soda.models;

import java.time.ZonedDateTime;
import java.util.List;

import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

@Value
@Jacksonized
@SuperBuilder
public class ScanResult {
    String definitionName;
    String defaultDataSource;
    ZonedDateTime dataTimestamp;
    ZonedDateTime scanStartTimestamp;
    ZonedDateTime scanEndTimestamp;
    Boolean hasErrors;
    Boolean hasWarnings;
    Boolean hasFailures;
    List<Metric> metrics;
    List<Check> checks;
    List<String> automatedMonitoringChecks;
    List<String> profiling;
    List<String> metadata;
    // List<Log> logs;
}
