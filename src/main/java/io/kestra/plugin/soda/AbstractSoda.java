package io.kestra.plugin.soda;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.tasks.runners.PluginUtilsService;
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

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractSoda extends Task {
    protected static final ObjectMapper MAPPER = JacksonMapper.ofYaml();
    private static final String REDACTED = "******";

    /**
     * Sensitive fragments matched as substrings of the normalized (alphanumeric-only, lowercased)
     * key. Substring matching deliberately errs toward over-redaction — any key merely containing
     * one of these (e.g. {@code keyfile}, {@code keyspace}, {@code client_secret}) is redacted — so
     * that a secret is never leaked into task Output at the cost of occasionally masking a benign
     * value. {@code key} already covers {@code api_key}, {@code access_key}, {@code private_key}, etc.
     */
    private static final Set<String> SENSITIVE_KEY_PATTERNS = Set.of(
        "password", "passwd", "pwd",
        "secret",
        "token",
        "key",
        "credential",
        "accountinfojson",
        "auth"
    );

    @Schema(
        title = "Runner to use",
        description = "Deprecated, use 'taskRunner' instead"
    )
    @Deprecated
    @PluginProperty(group = "execution")
    protected Property<RunnerType> runner;

    @Schema(
        title = "Deprecated, use 'taskRunner' instead"
    )
    @PluginProperty(group = "execution")
    @Deprecated
    private DockerOptions docker;

    @Schema(
        title = "The task runner to use",
        description = "Task runners are provided by plugins, each have their own properties."
    )
    @PluginProperty(group = "execution")
    @Builder.Default
    @Valid
    private TaskRunner<?> taskRunner = Docker.instance();

    @Schema(
        title = "The task runner container image, only used if the task runner is container-based",
        description = "Defaults to the image returned by the task's `defaultImage()` (e.g. a pinned Soda Core 3 image for `Scan`, a plain Python image for `VerifyContract`)."
    )
    @PluginProperty(group = "execution")
    private Property<String> containerImage;

    @Schema(title = "Deprecated, use the `docker` property instead", deprecated = true)
    @PluginProperty(group = "advanced")
    @Deprecated
    private DockerOptions dockerOptions;

    @JsonSetter
    public void setDockerOptions(DockerOptions dockerOptions) {
        this.dockerOptions = dockerOptions;
        this.docker = dockerOptions;
    }

    @Schema(
        title = "Input files are extra files that will be available in the dbt working directory",
        description = "You can define the files as map or a JSON string. " +
            "Each file can be defined inlined or can reference a file from Kestra's internal storage."
    )
    @PluginProperty(group = "source", 
        additionalProperties = String.class,
        dynamic = true
    )
    private Object inputFiles;

    @Schema(
        title = "List of python dependencies to add to the python execution process",
        description = "Python dependencies list to setup in the virtualenv, in the same format than requirements.txt. It must at least provides dbt."
    )
    @PluginProperty(group = "advanced")
    protected Property<List<String>> requirements;

    @Schema(
        title = "Additional environment variables for the current process"
    )
    @PluginProperty(group = "execution")
    protected Property<Map<String, String>> env;

    /**
     * Not {@code @NotNull} here: {@code configuration} is a Soda Core 3 (SodaCL) concept that only
     * {@link Scan} requires. Subclasses targeting Soda Core 4 (e.g. {@link VerifyContract}) use a
     * {@code dataSource} property instead and never set this field, so each subclass enforces its
     * own required-ness rather than the shared base class assuming everyone needs it.
     */
    @Schema(
        title = "The configuration file"
    )
    @PluginProperty(group = "main")
    Property<Map<String, Object>> configuration;

    protected abstract String defaultImage();

    protected Map<String, String> finalInputFiles(RunContext runContext, Path workingDirectory) throws IOException, IllegalVariableEvaluationException {
        Map<String, String> map = this.inputFiles != null ? new HashMap<>(PluginUtilsService.transformInputFiles(runContext, this.inputFiles)) : new HashMap<>();

        if (this.configuration != null) {
            var renderedConfig = runContext.render(configuration).asMap(String.class, Object.class);
            if (!renderedConfig.isEmpty()) {
                map.put("configuration.yml", MAPPER.writeValueAsString(renderedConfig));
            }
        }

        return map;
    }

    public CommandsWrapper start(RunContext runContext) throws Exception {
        var env = runContext.render(this.getEnv()).asMap(String.class, String.class);
        CommandsWrapper commandsWrapper = new CommandsWrapper(runContext)
            .withEnv(env.isEmpty() ? new HashMap<>() : env)
            .withRunnerType(runContext.render(this.getRunner()).as(RunnerType.class).orElse(null))
            .withTaskRunner(this.taskRunner)
            .withContainerImage(runContext.render(this.getContainerImage()).as(String.class).orElse(this.defaultImage()))
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
            .addEnv(
                Map.of(
                    "PYTHONUNBUFFERED", "true",
                    "PIP_ROOT_USER_ACTION", "ignore"
                )
            )
            .withInterpreter(Property.ofValue(List.of("/bin/sh", "-c")))
            .withCommands(Property.ofExpression(JacksonMapper.ofJson().writeValueAsString(commands)));
    }

    private DockerOptions injectDefaults(DockerOptions original) {
        if (original == null) {
            return null;
        }

        var builder = original.toBuilder();
        if (original.getImage() == null) {
            builder.image(this.defaultImage());
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
            String installArgs = requirements.stream()
                .map(AbstractSoda::shellQuote)
                .collect(Collectors.joining(" "));

            renderer.addAll(
                Arrays.asList(
                    "./bin/pip install pip --upgrade > /dev/null",
                    "./bin/pip install " + installArgs + " > /dev/null"
                )
            );
        }

        return String.join("\n", renderer);
    }

    /**
     * Wraps a value in single quotes for safe interpolation into a {@code /bin/sh -c} command line.
     * Inside single quotes the shell treats every character literally, so all metacharacters
     * ({@code ; & | > < * $ ` ( )} …) are neutralized while any valid {@code requirements.txt} syntax
     * (version specifiers, extras, VCS URLs, environment markers) is preserved verbatim. The only
     * character that cannot appear inside single quotes — the single quote itself — is escaped with
     * the standard {@code '\''} sequence (close quote, escaped quote, reopen quote).
     */
    private static String shellQuote(String value) {
        return "'" + value.replace("'", "'\\''") + "'";
    }

    /**
     * Recursively scrubs sensitive leaf values (passwords, tokens, keys, credentials, etc.) from a
     * rendered connection map before it is stored in task Output, which is persisted in execution
     * state and visible to any user with read access to the execution. Shared by every subclass
     * that echoes its connection map back in Output ({@link Scan}'s {@code configuration},
     * {@link VerifyContract}'s {@code dataSource}).
     */
    @SuppressWarnings("unchecked")
    protected static Map<String, Object> scrubSensitiveValues(Map<String, Object> map) {
        Map<String, Object> scrubbed = new LinkedHashMap<>();

        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            if (isSensitiveKey(key)) {
                scrubbed.put(key, REDACTED);
            } else {
                scrubbed.put(key, scrubValue(value));
            }
        }

        return scrubbed;
    }

    /**
     * Recurses through container values so sensitive keys nested inside maps <em>or lists</em>
     * (e.g. a list of connection maps) are scrubbed too; scalar values are returned unchanged.
     */
    @SuppressWarnings("unchecked")
    private static Object scrubValue(Object value) {
        if (value instanceof Map) {
            return scrubSensitiveValues((Map<String, Object>) value);
        }

        if (value instanceof List) {
            List<Object> scrubbedList = new ArrayList<>();
            for (Object element : (List<Object>) value) {
                scrubbedList.add(scrubValue(element));
            }
            return scrubbedList;
        }

        return value;
    }

    private static boolean isSensitiveKey(String key) {
        if (key == null) {
            return false;
        }

        String normalized = key.toLowerCase().replaceAll("[^a-z0-9]", "");
        for (String pattern : SENSITIVE_KEY_PATTERNS) {
            if (normalized.contains(pattern)) {
                return true;
            }
        }

        return false;
    }
}
