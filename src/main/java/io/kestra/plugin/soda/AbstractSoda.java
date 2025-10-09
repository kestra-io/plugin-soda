package io.kestra.plugin.soda;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.tasks.runners.PluginUtilsService;
import io.kestra.core.models.tasks.runners.ScriptService;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.RunnerType;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.kestra.plugin.scripts.runner.docker.Docker;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
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
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractSoda extends Task {
    private static final String DEFAULT_IMAGE = "sodadata/soda-core";
    protected static final ObjectMapper MAPPER = JacksonMapper.ofYaml();

    @Schema(
        title = "Runner to use",
        description = "Deprecated, use 'taskRunner' instead"
    )
    @Deprecated
    protected Property<RunnerType> runner;

    @Schema(
        title = "Deprecated, use 'taskRunner' instead"
    )
    @PluginProperty
    @Deprecated
    private DockerOptions docker;

    @Schema(
        title = "The task runner to use.",
        description = "Task runners are provided by plugins, each have their own properties."
    )
    @PluginProperty
    @Builder.Default
    @Valid
    private TaskRunner<?> taskRunner = Docker.instance();

    @Schema(title = "The task runner container image, only used if the task runner is container-based.")
    @Builder.Default
    private Property<String> containerImage = Property.of(DEFAULT_IMAGE);

    @Schema(title = "Deprecated, use the `docker` property instead", deprecated = true)
    @PluginProperty
    @Deprecated
    private DockerOptions dockerOptions;

    @JsonSetter
    public void setDockerOptions(DockerOptions dockerOptions) {
        this.dockerOptions = dockerOptions;
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
    protected Property<List<String>> requirements;

    @Schema(
        title = "Additional environment variables for the current process."
    )
    protected Property<Map<String, String>> env;

    @Schema(
        title = "The configuration file"
    )
    @NotNull
    Property<Map<String, Object>> configuration;

    protected Map<String, String> finalInputFiles(RunContext runContext, Path workingDirectory) throws IOException, IllegalVariableEvaluationException {
        Map<String, String> map = this.inputFiles != null ? new HashMap<>(PluginUtilsService.transformInputFiles(runContext, this.inputFiles)) : new HashMap<>();

        var renderedConfig = runContext.render(configuration).asMap(String.class, Object.class);
        if (!renderedConfig.isEmpty()) {
            map.put("configuration.yml", MAPPER.writeValueAsString(renderedConfig));
        }

        return map;
    }

    public CommandsWrapper start(RunContext runContext) throws Exception {
        var env = runContext.render(this.getEnv()).asMap(String.class, String.class);
        CommandsWrapper commandsWrapper = new CommandsWrapper(runContext)
            .withEnv(env.isEmpty() ? new HashMap<>() : env)
            .withRunnerType(runContext.render(this.getRunner()).as(RunnerType.class).orElse(null))
            .withTaskRunner(this.taskRunner)
            .withContainerImage(runContext.render(this.getContainerImage()).as(String.class).orElse(null))
            .withOutputFiles(List.of("result.json"))
            .withDockerOptions(injectDefaults(this.getDocker()));
        Path workingDirectory = commandsWrapper.getWorkingDirectory();

        List<String> commands = new ArrayList<>();
        if (this.requirements != null) {
            commands.add(this.virtualEnvCommand(runContext, workingDirectory, runContext.render(this.requirements).asList(String.class)));
            commands.add("./bin/python {{workingDir}}/main.py");
        } else {
            commands.add("python {{workingDir}}/main.py");
        }


        PluginUtilsService.createInputFiles(
            runContext,
            workingDirectory,
            this.finalInputFiles(runContext, workingDirectory),
            this.taskRunner.additionalVars(runContext, commandsWrapper)
        );

        return commandsWrapper
            .addEnv(Map.of(
                "PYTHONUNBUFFERED", "true",
                "PIP_ROOT_USER_ACTION", "ignore"
            ))
            .withInterpreter(Property.of(List.of("/bin/sh", "-c")))
            .withCommands(new Property<>(JacksonMapper.ofJson().writeValueAsString(commands)));
    }

    private DockerOptions injectDefaults(DockerOptions original) {
        if (original == null) {
            return null;
        }

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
