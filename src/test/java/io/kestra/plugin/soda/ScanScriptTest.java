package io.kestra.plugin.soda;

import java.nio.file.Files;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class ScanScriptTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void verboseAndVariablesAreOnSeparateLines() throws Exception {
        var lines = generatedScript(true, Map.of("country", "FR"));

        assertThat(lines, hasItem("scan.set_verbose()"));
        assertThat(lines, hasItem(startsWith("scan.add_variables(json.loads(")));
        assertThat(lines, not(hasItem(containsString("set_verbose()scan."))));
    }

    @Test
    void variablesOnlyEndsItsLine() throws Exception {
        var lines = generatedScript(false, Map.of("country", "FR"));

        assertThat(lines, not(hasItem(containsString("set_verbose"))));
        assertThat(lines, hasItem(startsWith("scan.add_variables(json.loads(")));
        assertThat(lines, hasItem("result = scan.execute()"));
    }

    @Test
    void verboseOnlyEndsItsLine() throws Exception {
        var lines = generatedScript(true, null);

        assertThat(lines, hasItem("scan.set_verbose()"));
        assertThat(lines, not(hasItem(containsString("add_variables"))));
        assertThat(lines, hasItem("result = scan.execute()"));
    }

    private List<String> generatedScript(boolean verbose, Map<String, Object> variables) throws Exception {
        var builder = Scan.builder()
            .id("unit-test")
            .type(Scan.class.getName())
            .configuration(
                Property.ofValue(
                    Map.of(
                        "data_source kestra", Map.of("type", "duckdb", "path", "orders.duckdb")
                    )
                )
            )
            .checks(Map.of("checks for orders", List.of("row_count > 0")))
            .verbose(Property.ofValue(verbose));

        if (variables != null) {
            builder.variables(Property.ofValue(variables));
        }

        var task = builder.build();
        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        var workingDirectory = Files.createTempDirectory("soda-scan-script");

        var files = task.finalInputFiles(runContext, workingDirectory);

        return files.get("main.py").lines().toList();
    }
}
