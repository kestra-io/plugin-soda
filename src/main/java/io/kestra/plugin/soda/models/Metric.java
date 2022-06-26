package io.kestra.plugin.soda.models;

import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

@Value
@Jacksonized
@SuperBuilder
public class Metric {
    String identity;
    String metricName;
    Double value;
}

