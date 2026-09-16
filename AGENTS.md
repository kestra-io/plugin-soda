# Kestra Soda Plugin

## What

- Provides plugin components under `io.kestra.plugin.soda`.
- Includes classes such as `Scan` (deprecated, Soda Core 3), `VerifyContract` (Soda Core 4), `ScanResult`, `ContractVerificationResult`, `DiscoverTablesResultTable`, `Metric`.

## Why

- What user problem does this solve? Teams need to run Soda scans for data quality checks from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps Soda steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on Soda.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `soda`

### Key Plugin Classes

- `io.kestra.plugin.soda.Scan` (deprecated — Soda Core 3 / SodaCL, pinned to `sodadata/soda-core:v3.5.2`)
- `io.kestra.plugin.soda.VerifyContract` (Soda Core 4 data contracts, default image `python:3.12-slim`)
- `io.kestra.plugin.soda.AbstractSoda` (shared base: task runner, `requirements`/venv setup, sensitive-value scrubbing)

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
