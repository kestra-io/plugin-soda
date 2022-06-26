package io.kestra.plugin.soda.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

import java.util.List;
import java.util.Map;

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

