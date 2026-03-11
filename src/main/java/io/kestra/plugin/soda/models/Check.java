package io.kestra.plugin.soda.models;

import java.util.List;

import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

@Value
@Jacksonized
@SuperBuilder
public class Check {
    String identity;
    String name;
    String type;
    String definition;
    // String location;
    String dataSource;
    String table;
    String column;
    List<String> metrics;
    CheckOutcome outcome;
    // String diagnostics;
}
