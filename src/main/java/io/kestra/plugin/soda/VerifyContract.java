package io.kestra.plugin.soda;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.kestra.plugin.soda.models.ContractVerificationResult;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Verify a Soda Core 4 data contract and report results",
    description = "Verifies one or more Soda Core 4 data contracts (`dataset` + `columns` + `checks` YAML) against the provided data source, writes the verification result to internal storage, and marks the task according to the outcome. " +
        "Unlike `Scan`, this task targets Soda Core 4, which replaced SodaCL scans with data contracts and has no official Docker image yet — the default `python:3.12-slim` image expects `requirements` to install `soda-core` and the relevant connector (e.g. `soda-duckdb`, `soda-postgres`)."
)
@Plugin(
    examples = {
        @Example(
            title = "Seed a DuckDB database and verify a data contract against it.",
            full = true,
            code = """
                id: soda_verify_contract_duckdb
                namespace: company.team

                tasks:
                  - id: seed_orders
                    type: io.kestra.plugin.scripts.python.Script
                    containerImage: python:3.12-slim
                    beforeCommands:
                      - pip install duckdb --quiet
                    outputFiles:
                      - orders.duckdb
                    script: |
                      import duckdb

                      connection = duckdb.connect("orders.duckdb")
                      connection.execute("CREATE TABLE orders (id INTEGER, amount DOUBLE)")
                      connection.execute("INSERT INTO orders VALUES (1, 42.5), (2, 108.0), (3, 15.25)")
                      connection.close()

                  - id: verify_orders_contract
                    type: io.kestra.plugin.soda.VerifyContract
                    inputFiles:
                      orders.duckdb: "{{ outputs.seed_orders.outputFiles['orders.duckdb'] }}"
                    requirements:
                      - soda-core==4.23.1
                      - soda-duckdb==4.23.1
                    dataSource:
                      type: duckdb
                      name: kestra
                      connection:
                        database: orders.duckdb
                    contracts:
                      - dataset: kestra/orders
                        columns:
                          - name: id
                          - name: amount
                            checks:
                              - missing: {}
                        checks:
                          - row_count: {}
                """
        )
    }
)
public class VerifyContract extends AbstractSoda implements RunnableTask<VerifyContract.Output> {
    private static final String DEFAULT_IMAGE = "python:3.12-slim";

    @Schema(
        title = "Data source connection",
        description = "Required map rendered to `data_source.yml`: a Soda Core 4 data source definition (`type`, `name`, `connection`). This replaces `configuration` for Soda Core 4 — do not pass a Soda Core 3 `data_source <name>:` block here, use `io.kestra.plugin.soda.Scan` for that."
    )
    @PluginProperty(group = "main")
    @NotNull
    Property<Map<String, Object>> dataSource;

    @Schema(
        title = "Data contracts to verify",
        description = "Required list of Soda Core 4 data contracts, each rendered to its own `contract-N.yml` file. Each contract is a map with `dataset` (a `<data source name>/<dataset name>` reference, e.g. `kestra/orders`), `columns`, and `checks` keys — not SodaCL `checks for <table>:` syntax. Column-scoped checks (e.g. `missing`) go under the column's own `checks` list; dataset-scoped checks (e.g. `row_count`) go under the contract's top-level `checks` list. Use `{}` rather than a bare trailing colon for a no-argument check (e.g. `row_count: {}`, not `row_count:`) — a bare colon is parsed as an empty string instead of `null` and Soda then rejects it as an invalid check definition."
    )
    @PluginProperty(group = "main")
    @NotNull
    Property<List<Map<String, Object>>> contracts;

    @Schema(
        title = "Runtime variables",
        description = "Optional variables injected into the contract verification for templating the data source or contracts; values are rendered by Kestra before execution."
    )
    @PluginProperty(group = "main")
    Property<Map<String, Object>> variables;

    @Schema(
        title = "Enable verbose logging",
        description = "Defaults to false; when true, the contract verification runs with debug-level logging."
    )
    @PluginProperty(group = "advanced")
    @Builder.Default
    Property<Boolean> verbose = Property.ofValue(false);

    @Override
    protected String defaultImage() {
        return DEFAULT_IMAGE;
    }

    @Override
    protected Map<String, String> finalInputFiles(RunContext runContext, Path workingDirectory) throws IOException, IllegalVariableEvaluationException {
        var rRequirements = runContext.render(this.getRequirements()).asList(String.class);
        if (rRequirements.isEmpty()) {
            throw new IllegalArgumentException(
                "`requirements` is required and must at least include `soda-core` and a connector package (e.g. `soda-duckdb`, `soda-postgres`), " +
                    "since the default `python:3.12-slim` image does not bundle Soda Core."
            );
        }

        var rDataSource = runContext.render(this.dataSource).asMap(String.class, Object.class);
        if (rDataSource.isEmpty()) {
            throw new IllegalArgumentException("`dataSource` is required and must be a Soda Core 4 data source definition (`type`, `name`, `connection`).");
        }
        if (rDataSource.keySet().stream().anyMatch(key -> key.startsWith("data_source "))) {
            throw new IllegalArgumentException(
                "`dataSource` looks like a Soda Core 3 `data_source <name>:` block rather than a Soda Core 4 data source definition. " +
                    "Use `io.kestra.plugin.soda.Scan` to run Soda Core 3 scans, or rewrite `dataSource` as a flat `type`/`name`/`connection` map."
            );
        }

        var rContracts = runContext.render(this.contracts).asList(Map.class);
        if (rContracts.isEmpty()) {
            throw new IllegalArgumentException("`contracts` is required and must contain at least one Soda Core 4 data contract (`dataset` + `columns` + `checks`).");
        }

        Map<String, String> map = super.finalInputFiles(runContext, workingDirectory);
        map.put("data_source.yml", MAPPER.writeValueAsString(rDataSource));

        List<String> contractFiles = new ArrayList<>();
        for (var i = 0; i < rContracts.size(); i++) {
            @SuppressWarnings("unchecked")
            Map<String, Object> contract = (Map<String, Object>) rContracts.get(i);

            if (contract.keySet().stream().anyMatch(key -> key.startsWith("checks for "))) {
                throw new IllegalArgumentException(
                    "Contract at index " + i + " looks like SodaCL `checks for <table>:` syntax rather than a Soda Core 4 data contract. " +
                        "Use `io.kestra.plugin.soda.Scan` for SodaCL checks, or rewrite it as a `dataset` + `columns` + `checks` contract."
                );
            }
            if (!contract.containsKey("dataset")) {
                throw new IllegalArgumentException("Contract at index " + i + " is missing the required `dataset` key. Expected shape: `dataset`, `columns`, `checks`.");
            }

            var fileName = "contract-" + i + ".yml";
            map.put(fileName, MAPPER.writeValueAsString(contract));
            contractFiles.add(fileName);
        }

        map.put("main.py", buildMainScript(runContext, contractFiles));

        return map;
    }

    /**
     * Field/property names below (``check_collection.data_source_name``/``dataset_name``,
     * ``check_results``, ``check.column_name``, ``outcome.value``, ``is_failed``/``is_warned``/
     * ``has_errors``) are verified against a real soda-core 4.23.1 + soda-duckdb install, not
     * guessed — see the PR description for how they were confirmed.
     */
    private String buildMainScript(RunContext runContext, List<String> contractFiles) throws IllegalVariableEvaluationException, IOException {
        var contractFilesLiteral = contractFiles.stream()
            .map(file -> "ContractYamlSource.from_file_path(\"{{workingDir}}/" + file + "\")")
            .reduce((a, b) -> a + ", " + b)
            .orElse("");

        var main = "import json\n" +
            "import logging\n" +
            "\n" +
            "from soda_core.contracts.contract_verification import ContractVerificationSession\n" +
            "from soda_core.common.yaml import ContractYamlSource, DataSourceYamlSource\n" +
            "\n";

        main += "logging.basicConfig(level=logging." + (runContext.render(verbose).as(Boolean.class).orElse(false) ? "DEBUG" : "INFO") + ")\n\n";

        main += "def _serialize_check(check_result):\n" +
            "    check = check_result.check\n" +
            "    return {\n" +
            "        'name': check.name,\n" +
            "        'type': check.type,\n" +
            "        'column': check.column_name,\n" +
            "        'definition': check.definition,\n" +
            "        'outcome': check_result.outcome.value.lower(),\n" +
            "    }\n" +
            "\n" +
            "def _serialize_contract(result):\n" +
            "    return {\n" +
            "        'dataSource': result.check_collection.data_source_name,\n" +
            "        'dataset': result.check_collection.dataset_name,\n" +
            "        'checks': [_serialize_check(check_result) for check_result in result.check_results],\n" +
            "        'hasFailures': result.is_failed,\n" +
            "        'hasWarnings': result.is_warned,\n" +
            "        'hasErrors': result.has_errors,\n" +
            "    }\n" +
            "\n";

        main += "session_result = ContractVerificationSession.execute(\n" +
            "    contract_yaml_sources=[" + contractFilesLiteral + "],\n" +
            "    data_source_yaml_sources=[DataSourceYamlSource.from_file_path(\"{{workingDir}}/data_source.yml\")],\n";

        if (variables != null) {
            // JSON booleans/null (true/false/null) are not valid Python literals (Python needs
            // True/False/None), so the rendered variables cannot be spliced directly into a Python
            // dict-literal position. Instead, embed the JSON text as a Python string literal (by
            // JSON-encoding it a second time, which produces valid Python string-escaping too) and
            // parse it at runtime with `json.loads`, letting Python's own JSON parser produce the
            // correct True/False/None values.
            String variablesJson = JacksonMapper.ofJson().writeValueAsString(runContext.render(variables).asMap(String.class, Object.class));
            String variablesPythonLiteral = JacksonMapper.ofJson().writeValueAsString(variablesJson);
            main += "    variables=json.loads(" + variablesPythonLiteral + "),\n";
        }

        main += ")\n\n";

        main += "contract_results = [_serialize_contract(result) for result in session_result.contract_verification_results]\n" +
            "has_failures = session_result.is_failed\n" +
            "has_warnings = session_result.is_warned\n" +
            "has_errors = session_result.has_errors\n" +
            "\n" +
            "if has_errors:\n" +
            "    exit_code = 3\n" +
            "elif has_failures:\n" +
            "    exit_code = 1\n" +
            "elif has_warnings:\n" +
            "    exit_code = 2\n" +
            "else:\n" +
            "    exit_code = 0\n" +
            "\n" +
            "payload = {\n" +
            "    'contractResults': contract_results,\n" +
            "    'hasFailures': has_failures,\n" +
            "    'hasWarnings': has_warnings,\n" +
            "    'hasErrors': has_errors,\n" +
            "}\n" +
            "\n" +
            "with open('{{workingDir}}/result.json', 'w') as out:\n" +
            "    out.write(json.dumps(payload))\n" +
            "\n" +
            "print('::{\"outputs\": {\"exitCode\":', exit_code, '}}::')";

        return main;
    }

    @Override
    public VerifyContract.Output run(RunContext runContext) throws Exception {
        CommandsWrapper commandsWrapper = this.start(runContext);
        ScriptOutput output = commandsWrapper.run();

        ContractVerificationResult result = parseResult(runContext, output);

        var checkCount = result.getContractResults() == null ? 0 : result.getContractResults().stream()
            .mapToInt(contract -> contract.getChecks() == null ? 0 : contract.getChecks().size())
            .sum();

        return Output.builder()
            .result(result)
            .stdOutLineCount(output.getStdOutLineCount())
            .stdErrLineCount(output.getStdErrLineCount())
            .dataSource(scrubSensitiveValues(runContext.render(this.dataSource).asMap(String.class, Object.class)))
            .checkCount(checkCount)
            .hasFailures(Boolean.TRUE.equals(result.getHasFailures()))
            .hasWarnings(Boolean.TRUE.equals(result.getHasWarnings()))
            .hasErrors(Boolean.TRUE.equals(result.getHasErrors()))
            .exitCode((Integer) output.getVars().get("exitCode"))
            .build();
    }

    protected ContractVerificationResult parseResult(RunContext runContext, ScriptOutput output) throws IOException {
        return JacksonMapper.ofJson(false).readValue(
            runContext.storage().getFile(output.getOutputFiles().get("result.json")),
            ContractVerificationResult.class
        );
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Contract verification results payload",
            description = "Parsed Soda Core 4 contract verification outcome, with one entry per contract and per-check outcomes."
        )
        private final ContractVerificationResult result;

        @Schema(
            title = "Standard output line count",
            description = "Number of lines captured from stdout during the contract verification."
        )
        private final int stdOutLineCount;

        @Schema(
            title = "Standard error line count",
            description = "Number of lines captured from stderr during the contract verification."
        )
        private final int stdErrLineCount;

        @Schema(
            title = "Contract verification process exit code",
            description = "0 no issues, 1 check failures, 2 check warnings, 3 log/session errors, 4 results not sent to Soda Cloud (not treated as a failure by this task)."
        )
        @NotNull
        private final int exitCode;

        @Schema(
            title = "Total number of checks evaluated",
            description = "Sum of checks evaluated across every contract in this verification session."
        )
        private final int checkCount;

        @Schema(
            title = "Whether any check failed"
        )
        private final boolean hasFailures;

        @Schema(
            title = "Whether any check produced a warning"
        )
        private final boolean hasWarnings;

        @Schema(
            title = "Whether verification raised an error independent of check outcomes"
        )
        private final boolean hasErrors;

        @Schema(
            title = "Rendered data source",
            description = "Data source connection map applied to the contract verification, as rendered by Kestra expressions."
        )
        @NotNull
        private final Map<String, Object> dataSource;

        @Override
        public Optional<State.Type> finalState() {
            return Optional.of(switch (this.exitCode) {
                case 0 -> State.Type.SUCCESS;
                case 2, 4 -> State.Type.WARNING;
                case 1, 3 -> State.Type.FAILED;
                default -> State.Type.FAILED;
            });
        }
    }
}
