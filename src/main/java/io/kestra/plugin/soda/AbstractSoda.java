package io.kestra.plugin.soda;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.tasks.PluginUtilsService;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.RunnerType;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.kestra.plugin.scripts.exec.scripts.services.ScriptService;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractSoda extends Task {
    private static final String DEFAULT_IMAGE = "sodadata/soda-core";
    protected static final ObjectMapper MAPPER = JacksonMapper.ofYaml();

    @Builder.Default
    @Schema(
        title = "Runner to use"
    )
    @PluginProperty
    @NotNull
    @NotEmpty
    protected RunnerType runner = RunnerType.DOCKER;

    @Schema(
        title = "Docker options for the `DOCKER` runner",
        defaultValue = "{image=" + DEFAULT_IMAGE + ", pullPolicy=ALWAYS}"
    )
    @PluginProperty
    @Builder.Default
    private DockerOptions docker = DockerOptions.builder().build();

    @Schema(title = "Deprecated, use the `docker` property instead", deprecated = true)
    @PluginProperty
    @Deprecated
    public DockerOptions getDockerOptions() {
        return docker;
    }

    @Deprecated
    public void setDockerOptions(DockerOptions dockerOptions) {
        this.docker = dockerOptions;
    }

    @Schema(
        title = "Input files are extra files that will be available in the dbt working directory.",
        description = "You can define the files as map or a JSON string. " +
            "Each file can be defined inlined or can reference a file from Kestra's internal storage."
    )
    @PluginProperty(
        additionalProperties = String.class,
        dynamic = true
    )
    private Object inputFiles;

    @Schema(
        title = "List of python dependencies to add to the python execution process",
        description = "Python dependencies list to setup in the virtualenv, in the same format than requirements.txt. It must at least provides dbt."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    protected List<String> requirements;

    @Schema(
        title = "Additional environment variables for the current process."
    )
    @PluginProperty(
        additionalProperties = String.class,
        dynamic = true
    )
    protected Map<String, String> env;

    @Schema(
        title = "The configuration file"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    @NotEmpty
    Map<String, Object> configuration;

    protected Map<String, String> finalInputFiles(RunContext runContext, Path workingDirectory) throws IOException, IllegalVariableEvaluationException {
        Map<String, String> map = this.inputFiles != null ? new HashMap<>(PluginUtilsService.transformInputFiles(runContext, this.inputFiles)) : new HashMap<>();

        map.put("configuration.yml", MAPPER.writeValueAsString(runContext.render(configuration)));

        return map;
    }

    public CommandsWrapper start(RunContext runContext) throws Exception {
        CommandsWrapper commandsWrapper = new CommandsWrapper(runContext)
            .withEnv(this.getEnv())
            .withRunnerType(this.getRunner())
            .withDockerOptions(injectDefaults(this.getDocker()));
        Path workingDirectory = commandsWrapper.getWorkingDirectory();

        List<String> commands = new ArrayList<>();
        if (this.requirements != null) {
            commands.add(this.virtualEnvCommand(runContext, workingDirectory, this.requirements));
            commands.add("./bin/python main.py");
        } else {
            commands.add("python main.py");
        }


        PluginUtilsService.createInputFiles(
            runContext,
            workingDirectory,
            this.finalInputFiles(runContext, workingDirectory),
            Collections.emptyMap()
        );

        List<String> commandsArgs = ScriptService.scriptCommands(
            List.of("/bin/sh", "-c"),
            null,
            commands
        );

        return commandsWrapper
            .addEnv(Map.of("PYTHONUNBUFFERED", "true"))
            .withCommands(commandsArgs);
    }

    private DockerOptions injectDefaults(DockerOptions original) {
        var builder = original.toBuilder();
        if (original.getImage() == null) {
            builder.image(DEFAULT_IMAGE);
        }
        if (original.getEntryPoint() == null) {
            builder.entryPoint(Collections.emptyList());
        }

        return builder.build();
    }

    private String virtualEnvCommand(RunContext runContext, Path workingDirectory, List<String> requirements) throws IllegalVariableEvaluationException {
        List<String> renderer = new ArrayList<>();

        renderer.add("set -o errexit");
        renderer.add("python -m venv --system-site-packages " + workingDirectory + " > /dev/null");

        if (requirements != null) {
            renderer.addAll(Arrays.asList(
                "./bin/pip install pip --upgrade > /dev/null",
                "./bin/pip install " + runContext.render(String.join(" ", requirements)) + " > /dev/null"));
        }

        return String.join("\n", renderer);
    }
}
