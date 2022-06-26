package io.kestra.plugin.soda.models;

import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

import java.time.ZonedDateTime;
import java.util.List;

@Value
@Jacksonized
@SuperBuilder
public class Log {
    String level;
    String message;
    ZonedDateTime timestamp;
    Integer index;
}

