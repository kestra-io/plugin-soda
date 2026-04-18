# Kestra Soda Plugin

## What

- Provides plugin components under `io.kestra.plugin.soda`.
- Includes classes such as `Scan`, `ScanResult`, `DiscoverTablesResultTable`, `Metric`.

## Why

- This plugin integrates Kestra with Soda.
- It provides tasks that run Soda scans for data quality checks.

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
