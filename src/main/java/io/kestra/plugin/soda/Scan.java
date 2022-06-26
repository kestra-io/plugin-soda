package io.kestra.plugin.soda;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.tasks.scripts.ScriptOutput;
import io.kestra.plugin.soda.models.ScanResult;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Run a soda scan"
)
@Plugin(
    examples = {
        @Example(
            title = "Run a scan on BigQuery",
            code = {
                "configuration:",
                "  data_source kestra:",
                "    type: bigquery",
                "    connection:",
                "      project_id: kestra-unit-test",
                "      dataset: demo",
                "      account_info_json: |",
                "        { YOUR JSON SERVICE ACCOUNT KEY }",
                "checks:",
                "  checks for orderDetail:",
                "  - row_count > 0",
                "  - max(unitPrice):",
                "      warn: when between 1 and 250",
                "      fail: when > 250",
                "  checks for territory:",
                "  - row_count > 0",
                "  - failed rows:",
                "      name: Failed rows query test",
                "      fail condition: regionId = 4",
                "requirements:",
                " - soda-core-bigquery"
            }
        )
    }
)
public class Scan extends AbstractSoda implements RunnableTask<Scan.Output> {
    @Schema(
        title = "The checks file"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    @NotEmpty
    Map<String, Object> checks;

    @Schema(
        title = "The variables to pass"
    )
    @PluginProperty(dynamic = true)
    Map<String, Object> variables;

    @Schema(
        title = "Enable verbose logging"
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    Boolean verbose = false;

    @Override
    protected Map<String, String> finalInputFiles(RunContext runContext) throws IOException, IllegalVariableEvaluationException {
        Map<String, String> map = super.finalInputFiles(runContext);

        String main = "import sys\n" +
            "import json\n" +
            "from soda.scan import Scan\n" +
            "from soda.soda_cloud.soda_cloud import SodaCloud\n" +
            "from soda.common.logs import configure_logging\n" +
            "\n" +
            "configure_logging()\n" +
            "\n" +
            "scan = Scan()\n" +
            "scan.set_data_source_name(\"kestra\")\n" +
            "scan.add_configuration_yaml_file(file_path=\"" + this.workingDirectory.toAbsolutePath() + "/configuration.yml\")\n" +
            "scan.add_sodacl_yaml_file(\"" + this.workingDirectory.toAbsolutePath() + "/checks.yml\")\n" +
            "\n";

        if (verbose) {
            main += "scan.set_verbose()";
        }

        if (variables != null) {
            main += "scan.add_variables(" + JacksonMapper.ofJson().writeValueAsString(runContext.render(variables)) + ")";
        }

        main += "\n" +
            "result = scan.execute()\n" +
            "\n" +
            "with open('" + this.workingDirectory.toAbsolutePath() + "/result.json', 'w') as out:\n" +
            "    out.write(json.dumps(SodaCloud.build_scan_results(scan)))\n" +
            "\n" +
            "print('::{\"outputs\": {\"exitCode\":', result, '}}::')";

        map.put("main.py", main);
        map.put("checks.yml", MAPPER.writeValueAsString(runContext.render(checks)));

        return map;
    }

    @Override
    public Scan.Output run(RunContext runContext) throws Exception {
        ScriptOutput start = this.start(runContext);

        ScanResult scanResult = parseResult(runContext);

        return Output.builder()
            .result(scanResult)
            .stdOutLineCount(start.getStdOutLineCount())
            .stdErrLineCount(start.getStdOutLineCount())
            .exitCode((Integer) start.getVars().get("exitCode"))
            .build();
    }

    protected ScanResult parseResult(RunContext runContext) throws IOException {
        ScanResult scanResult = JacksonMapper.ofJson(false).readValue(
            this.workingDirectory.resolve("result.json").toFile(),
            ScanResult.class
        );

        scanResult
            .getMetrics()
            .stream()
            .filter(metric -> metric.getValue() != null)
            .forEach(metric -> runContext.metric(Counter.of(
                metric.getIdentity(),
                metric.getValue(),
                "type", metric.getMetricName()
            )));

        return scanResult;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The scan result"
        )
        private final ScanResult result;

        @Schema(
            title = "The standard output line count"
        )
        private final int stdOutLineCount;

        @Schema(
            title = "The standard error line count"
        )
        private final int stdErrLineCount;

        @Schema(
            title = "The exit code of the whole execution"
        )
        @NotNull
        private final int exitCode;

        @Override
        public Optional<State.Type> finalState() {
            return Optional.of(this.result.getHasWarnings() ? State.Type.WARNING :
                (this.result.getHasFailures() || this.result.getHasErrors() ? State.Type.FAILED : State.Type.SUCCESS)
            );
        }
    }
}
