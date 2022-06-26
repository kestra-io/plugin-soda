package io.kestra.plugin.soda;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.tasks.scripts.AbstractPython;
import io.kestra.core.tasks.scripts.ScriptOutput;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.throwSupplier;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractSoda extends AbstractPython {
    static final protected ObjectMapper MAPPER = JacksonMapper.ofYaml();

    @Schema(
        title = "The configuration file"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    @NotEmpty
    Map<String, Object> configuration;


    @Override
    protected Map<String, String> finalInputFiles(RunContext runContext) throws IOException, IllegalVariableEvaluationException {
        Map<String, String> map = super.finalInputFiles(runContext);

        map.put("configuration.yml", MAPPER.writeValueAsString(runContext.render(configuration)));

        return map;
    }

    public ScriptOutput start(RunContext runContext) throws Exception {
        return run(runContext, throwSupplier(() -> {
            List<String> renderer = new ArrayList<>();
            renderer.add(this.virtualEnvCommand(runContext, this.requirements));
            renderer.add("./bin/python main.py");

            return String.join("\n", renderer);
        }));
    }
}
