package io.kestra.plugin.soda.models;

import java.time.ZonedDateTime;

import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

@Value
@Jacksonized
@SuperBuilder
public class Log {
    String level;
    String message;
    ZonedDateTime timestamp;
    Integer index;
}
