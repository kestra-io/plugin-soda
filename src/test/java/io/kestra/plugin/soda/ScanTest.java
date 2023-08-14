package io.kestra.plugin.soda;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.flows.State;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
class ScanTest {
    private static final TypeReference<Map<String, Object>> TYPE_REFERENCE = new TypeReference<>() {};

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {

        Scan task = Scan.builder()
            .id("unit-test")
            .type(Scan.class.getName())
            .configuration(JacksonMapper.ofYaml().readValue(
                "data_source kestra:\n" +
                    "  type: bigquery\n" +
                    "  connection:\n" +
                    "    project_id: \"kestra-unit-test\"\n" +
                    "    dataset: demo\n" +
                    "    account_info_json: |\n" +
                    "      " + StringUtils.replace(UtilsTest.serviceAccount(), "\n", "\n      "),
                TYPE_REFERENCE
            ))
            .checks(JacksonMapper.ofYaml().readValue("checks for orderDetail:\n" +
                    "  - row_count > 0\n" +
                    "  - max(unitPrice):\n" +
                    "      warn: when between 1 and 250\n" +
                    "      fail: when > 250\n",
                TYPE_REFERENCE
            ))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        Scan.Output runOutput = task.run(runContext);

        assertThat(runOutput.getResult().getHasFailures(), is(false));
        assertThat(runOutput.getResult().getHasErrors(), is(false));
        assertThat(runOutput.getResult().getHasWarnings(), is(true));
        assertThat(runOutput.finalState().get(), is(State.Type.WARNING));
    }

    @Test
    void failed() throws Exception {
        Scan task = Scan.builder()
            .id("unit-test")
            .type(Scan.class.getName())
            .configuration(JacksonMapper.ofYaml().readValue(
                "data_source kestra:\n" +
                    "  type: bigquery\n" +
                    "  connection:\n" +
                    "    project_id: \"kestra-unit-test\"\n" +
                    "    dataset: demo\n" +
                    "    account_info_json: |\n" +
                    "      " + StringUtils.replace(UtilsTest.serviceAccount(), "\n", "\n      "),
                TYPE_REFERENCE
            ))
            .checks(JacksonMapper.ofYaml().readValue("checks for orderDetail:\n" +
                    "checks for territory:\n" +
                    "  - row_count > 0\n" +
                    "  - failed rows:\n" +
                    "      name: Failed rows query test\n" +
                    "      fail condition: regionId = 4",
                TYPE_REFERENCE
            ))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        Scan.Output runOutput = task.run(runContext);

        assertThat(runOutput.getResult().getHasFailures(), is(true));
        assertThat(runOutput.getResult().getHasErrors(), is(false));
        assertThat(runOutput.getResult().getHasWarnings(), is(false));
        assertThat(runOutput.finalState().get(), is(State.Type.FAILED));
    }

    @Test
    void error() throws Exception {
        Scan task = Scan.builder()
            .id("unit-test")
            .type(Scan.class.getName())
            .configuration(JacksonMapper.ofYaml().readValue(
                "data_source kestra:\n" +
                    "  type: bigquery\n" +
                    "  connection:\n" +
                    "    project_id: \"kestra-unit-test\"\n" +
                    "    dataset: demo\n" +
                    "    account_info_json: |\n" +
                    "      " + StringUtils.replace(UtilsTest.serviceAccount(), "\n", "\n      "),
                TYPE_REFERENCE
            ))
            .checks(JacksonMapper.ofYaml().readValue("checks for orderDetail:\n" +
                    "checks for invalid_table:\n" +
                    "  - row_count > 0",
                TYPE_REFERENCE
            ))
            .requirements(List.of("soda-core-bigquery"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        Scan.Output runOutput = task.run(runContext);

        assertThat(runOutput.getResult().getHasFailures(), is(false));
        assertThat(runOutput.getResult().getHasErrors(), is(true));
        assertThat(runOutput.getResult().getHasWarnings(), is(false));
        assertThat(runOutput.finalState().get(), is(State.Type.FAILED));
    }
}
