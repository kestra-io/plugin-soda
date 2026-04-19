# Kestra Soda Plugin

## What

- Provides plugin components under `io.kestra.plugin.soda`.
- Includes classes such as `Scan`, `ScanResult`, `DiscoverTablesResultTable`, `Metric`.

## Why

- What user problem does this solve? Teams need to run Soda scans for data quality checks from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps Soda steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on Soda.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `soda`

### Key Plugin Classes

- `io.kestra.plugin.soda.Scan`

### Project Structure

```
plugin-soda/
├── src/main/java/io/kestra/plugin/soda/models/
├── src/test/java/io/kestra/plugin/soda/models/
├── build.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
