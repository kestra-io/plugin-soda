package io.kestra.plugin.soda;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.scripts.exec.scripts.models.RunnerType;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

@KestraTest
class AbstractSodaTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void deprecatedRunnerAndDockerYamlStillBind() throws Exception {
        var task = JacksonMapper.ofYaml().readValue("""
            id: scan
            type: io.kestra.plugin.soda.Scan
            runner: PROCESS
            docker:
              image: custom:1
            env:
              MY_VAR: value
            """, Scan.class);
        var runContext = runContextFactory.of(Map.of());

        assertThat(runContext.render(task.getRunnerType()).as(RunnerType.class).orElseThrow(() -> new IllegalStateException("`runner` did not bind")), is(RunnerType.PROCESS));
        assertThat(task.getDocker().getImage(), is("custom:1"));
        assertThat(runContext.render(task.getEnv()).asMap(String.class, String.class), is(Map.of("MY_VAR", "value")));
    }

    @Test
    void toStringDoesNotExposeEnvSecrets() {
        var task = Scan.builder()
            .id(IdUtils.create())
            .type(Scan.class.getName())
            .env(Property.ofValue(Map.of("SODA_API_KEY", "super-secret-value")))
            .build();

        assertThat(task.toString(), not(containsString("super-secret-value")));
    }
}
