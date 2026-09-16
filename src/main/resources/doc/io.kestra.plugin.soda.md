# How to use the Soda plugin

Run Soda data quality checks from Kestra flows inside a container.

## Common properties

`taskRunner` controls where the container runs — defaults to Docker. Add extra Python packages via `requirements`. Each task picks its own default `containerImage` — see below.

## Tasks

`Scan` (**deprecated**) runs a Soda Core 3 scan — set `configuration` as a map matching Soda's YAML connection config (data source type, connection details, and credentials). Set `checks` as a map of SodaCL check definitions. Data source credentials should reference [secrets](https://kestra.io/docs/concepts/secret) via Kestra's expression syntax. Pass extra files via `inputFiles` or pull them from [namespace files](https://kestra.io/docs/concepts/namespace-files). Set `variables` to pass runtime values into checks. The output includes `result` with pass/warn/fail outcomes per check, plus `hasErrors`, `hasWarnings`, and `hasFailures` flags. `containerImage` defaults to a pinned `sodadata/soda-core:v3.5.2` (Soda Core 3 is end-of-line and has no newer image). Apply runner properties globally with [plugin defaults](https://kestra.io/docs/workflow-components/plugin-defaults).

`VerifyContract` verifies Soda Core 4 data contracts — the SodaCL-based `Scan` replacement. Set `dataSource` as a map with `type`/`name`/`connection` (Soda Core 4 data source definition, not a 3.x `data_source <name>:` block). Set `contracts` as a list of contract maps, each with `dataset` (a `<data source name>/<dataset name>` reference, e.g. `kestra/orders`), `columns`, and `checks`. Soda Core 4 has no official Docker image yet, so `containerImage` defaults to plain `python:3.12-slim` and `requirements` must include `soda-core` plus the relevant connector (e.g. `soda-duckdb`, `soda-postgres`). The output includes `result` with per-contract, per-check outcomes, plus `hasErrors`, `hasWarnings`, `hasFailures`, and `checkCount` rollups. Migrating from `Scan` means rewriting SodaCL checks as data contracts — it is not a drop-in swap.
