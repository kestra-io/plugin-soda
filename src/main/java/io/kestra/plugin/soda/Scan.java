package io.kestra.plugin.soda;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.kestra.plugin.soda.models.ScanResult;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Run a Soda scan."
)
@Plugin(
    examples = {
        @Example(
            title = "Run a scan on BigQuery.",
            full = true,
            code = """
                   id: soda_scan
                   namespacae: company.team

                   tasks:
                     - id: scan
                       type: io.kestra.plugin.soda.Scan
                       configuration:
                         data_source kestra:
                           type: bigquery
                           connection:
                             project_id: kestra-unit-test
                             dataset: demo
                             account_info_json: |
                               {{ secret('GCP_CREDS') }}
                       checks:
                         checks for orderDetail:
                           - row_count > 0
                           - max(unitPrice):
                               warn: when between 1 and 250
                               fail: when > 250
                         checks for territory:
                           - row_count > 0
                           - failed rows:
                               name: Failed rows query test
                               fail condition: regionId = 4
                       requirements:
                         - soda-core-bigquery
                   """
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
    Property<Map<String, Object>> variables;

    @Schema(
        title = "Whether to enable verbose logging"
    )
    @Builder.Default
    Property<Boolean> verbose = Property.ofValue(false);

    @Override
    protected Map<String, String> finalInputFiles(RunContext runContext, Path workingDirectory) throws IOException, IllegalVariableEvaluationException {
        Map<String, String> map = super.finalInputFiles(runContext, workingDirectory);

        String main = "import sys\n" +
            "import json\n" +
            "from soda.scan import Scan\n" +
            "try:\n" +
            "   from soda.soda_cloud.soda_cloud import SodaCloud\n" +
            "except ImportError:\n" +
            "   from soda.cloud.soda_cloud import SodaCloud\n" +
            "from soda.common.logs import configure_logging\n" +
            "\n" +
            "configure_logging()\n" +
            "\n" +
            "scan = Scan()\n" +
            "scan.set_data_source_name(\"kestra\")\n" +
            "scan.add_configuration_yaml_file(file_path=\"{{workingDir}}/configuration.yml\")\n" +
            "scan.add_sodacl_yaml_file(\"{{workingDir}}/checks.yml\")\n" +
            "\n";

        if (runContext.render(verbose).as(Boolean.class).orElseThrow()) {
            main += "scan.set_verbose()";
        }

        if (variables != null) {
            main += "scan.add_variables(" + JacksonMapper.ofJson().writeValueAsString(runContext.render(variables).asMap(String.class, Object.class)) + ")";
        }

        main += "\n" +
            "result = scan.execute()\n" +
            "\n" +
            "with open('{{workingDir}}/result.json', 'w') as out:\n" +
            "    out.write(json.dumps(SodaCloud.build_scan_results(scan)))\n" +
            "\n" +
            "print('::{\"outputs\": {\"exitCode\":', result, '}}::')";

        map.put("main.py", main);
        map.put("checks.yml", MAPPER.writeValueAsString(runContext.render(checks)));

        return map;
    }

    @Override
    public Scan.Output run(RunContext runContext) throws Exception {
        CommandsWrapper commandsWrapper = this.start(runContext);
        ScriptOutput output = commandsWrapper.run();

        ScanResult scanResult = parseResult(runContext, output);

        return Output.builder()
            .result(scanResult)
            .stdOutLineCount(output.getStdOutLineCount())
            .stdErrLineCount(output.getStdOutLineCount())
            .configuration(runContext.render(configuration).asMap(String.class, Object.class))
            .exitCode((Integer) output.getVars().get("exitCode"))
            .build();
    }

    protected ScanResult parseResult(RunContext runContext, ScriptOutput output) throws IOException {
        ScanResult scanResult = JacksonMapper.ofJson(false).readValue(
            runContext.storage().getFile(output.getOutputFiles().get("result.json")),
            ScanResult.class
        );

        scanResult
            .getMetrics()
            .stream()
            .filter(metric -> metric.getValue() != null)
            .forEach(metric -> {
                Double metricValue = null;
                if (metric.getValue() instanceof Double) {
                    metricValue = (Double) metric.getValue();
                }

                if (metricValue != null) {
                    runContext.metric(Counter.of(
                        metric.getIdentity(),
                        metricValue,
                        "type", metric.getMetricName()
                    ));
                }
            });

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

        @Schema(
            title = "The used configuration"
        )
        @NotNull
        private Map<String, Object> configuration;

        @Override
        public Optional<State.Type> finalState() {
            return Optional.of(this.result.getHasWarnings() ? State.Type.WARNING :
                (this.result.getHasFailures() || this.result.getHasErrors() ? State.Type.FAILED : State.Type.SUCCESS)
            );
        }
    }
}
