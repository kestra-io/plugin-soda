package io.kestra.plugin.soda;

import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.scripts.runner.docker.Docker;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Targets DuckDB via {@code soda-duckdb} so the test needs no external service or credentials —
 * only Docker (to run the pinned {@code python:3.12-slim} image) and network access to pip-install
 * {@code soda-core}/{@code soda-duckdb}, mirroring {@code ScanTest}'s Docker-based integration
 * approach. {@code orders.duckdb} is a pre-seeded fixture (see {@code src/test/resources}) since
 * Soda Core 4's data source YAML has no way to run initialization SQL.
 */
@KestraTest
class VerifyContractTest {
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<>() {
    };
    private static final TypeReference<List<Map<String, Object>>> LIST_TYPE_REFERENCE = new TypeReference<>() {
    };

    @Inject
    private RunContextFactory runContextFactory;

    private static Map<String, Object> duckDbDataSource() throws Exception {
        return JacksonMapper.ofYaml().readValue(
            "type: duckdb\n" +
                "name: kestra\n" +
                "connection:\n" +
                "  database: orders.duckdb\n",
            MAP_TYPE_REFERENCE
        );
    }

    private static Map<String, String> inputFiles(RunContext runContext) throws Exception {
        try (InputStream fixture = VerifyContractTest.class.getClassLoader().getResourceAsStream("orders.duckdb")) {
            URI uri = runContext.storage().putFile(fixture, "orders.duckdb");
            return Map.of("orders.duckdb", uri.toString());
        }
    }

    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        VerifyContract task = VerifyContract.builder()
            .id("unit-test")
            .type(VerifyContract.class.getName())
            .taskRunner(Docker.builder().type(Docker.class.getName()).build())
            .inputFiles(inputFiles(runContext))
            .requirements(Property.ofValue(List.of("soda-core", "soda-duckdb")))
            .dataSource(Property.ofValue(duckDbDataSource()))
            .contracts(
                Property.ofValue(
                    JacksonMapper.ofYaml().readValue(
                        "- dataset: kestra/orders\n" +
                            "  columns:\n" +
                            "    - name: id\n" +
                            "      checks:\n" +
                            "        - missing: {}\n" +
                            "    - name: amount\n" +
                            "  checks:\n" +
                            "    - row_count:\n" +
                            "        threshold:\n" +
                            "          must_be_greater_than: 100\n" +
                            "          level: warn\n",
                        LIST_TYPE_REFERENCE
                    )
                )
            )
            .build();

        runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        VerifyContract.Output runOutput = task.run(runContext);

        assertThat(runOutput.isHasFailures(), is(false));
        assertThat(runOutput.isHasErrors(), is(false));
        assertThat(runOutput.isHasWarnings(), is(true));
        assertThat(runOutput.getCheckCount(), is(2));
        assertThat(runOutput.getResult().getContractResults(), hasSize(1));
        assertThat(runOutput.finalState().get(), is(State.Type.WARNING));
    }

    @Test
    void failed() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        VerifyContract task = VerifyContract.builder()
            .id("unit-test")
            .type(VerifyContract.class.getName())
            .taskRunner(Docker.builder().type(Docker.class.getName()).build())
            .inputFiles(inputFiles(runContext))
            .requirements(Property.ofValue(List.of("soda-core", "soda-duckdb")))
            .dataSource(Property.ofValue(duckDbDataSource()))
            .contracts(
                Property.ofValue(
                    JacksonMapper.ofYaml().readValue(
                        "- dataset: kestra/orders\n" +
                            "  columns:\n" +
                            "    - name: id\n" +
                            "    - name: amount\n" +
                            "  checks:\n" +
                            "    - row_count:\n" +
                            "        threshold:\n" +
                            "          must_be_greater_than: 100\n",
                        LIST_TYPE_REFERENCE
                    )
                )
            )
            .build();

        runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        VerifyContract.Output runOutput = task.run(runContext);

        assertThat(runOutput.isHasFailures(), is(true));
        assertThat(runOutput.isHasErrors(), is(false));
        assertThat(runOutput.isHasWarnings(), is(false));
        assertThat(runOutput.finalState().get(), is(State.Type.FAILED));
    }

    @Test
    void error() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        VerifyContract task = VerifyContract.builder()
            .id("unit-test")
            .type(VerifyContract.class.getName())
            .taskRunner(Docker.builder().type(Docker.class.getName()).build())
            .inputFiles(inputFiles(runContext))
            .requirements(Property.ofValue(List.of("soda-core", "soda-duckdb")))
            .dataSource(Property.ofValue(duckDbDataSource()))
            .contracts(
                Property.ofValue(
                    JacksonMapper.ofYaml().readValue(
                        "- dataset: kestra/does_not_exist\n" +
                            "  columns:\n" +
                            "    - name: id\n" +
                            "  checks:\n" +
                            "    - row_count: {}\n",
                        LIST_TYPE_REFERENCE
                    )
                )
            )
            .build();

        runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        VerifyContract.Output runOutput = task.run(runContext);

        assertThat(runOutput.isHasErrors(), is(true));
        assertThat(runOutput.isHasFailures(), is(false));
        assertThat(runOutput.finalState().get(), is(State.Type.FAILED));
    }

    @Test
    void redactsSensitiveDataSourceValuesInOutput() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Map<String, Object> dataSourceWithSecret = duckDbDataSource();
        @SuppressWarnings("unchecked")
        Map<String, Object> connection = (Map<String, Object>) dataSourceWithSecret.get("connection");
        connection.put("password", "hunter2");

        VerifyContract task = VerifyContract.builder()
            .id("unit-test")
            .type(VerifyContract.class.getName())
            .taskRunner(Docker.builder().type(Docker.class.getName()).build())
            .inputFiles(inputFiles(runContext))
            .requirements(Property.ofValue(List.of("soda-core", "soda-duckdb")))
            .dataSource(Property.ofValue(dataSourceWithSecret))
            .contracts(
                Property.ofValue(
                    JacksonMapper.ofYaml().readValue(
                        "- dataset: kestra/orders\n" +
                            "  columns:\n" +
                            "    - name: id\n" +
                            "  checks:\n" +
                            "    - row_count: {}\n",
                        LIST_TYPE_REFERENCE
                    )
                )
            )
            .build();

        runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        VerifyContract.Output runOutput = task.run(runContext);

        @SuppressWarnings("unchecked")
        Map<String, Object> outputConnection = (Map<String, Object>) runOutput.getDataSource().get("connection");
        assertThat(outputConnection.get("password"), is("******"));
        assertThat(outputConnection.get("database"), is("orders.duckdb"));
    }
}
