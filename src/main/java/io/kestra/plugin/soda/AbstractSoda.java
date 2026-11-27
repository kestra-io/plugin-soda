package io.kestra.plugin.soda;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.runners.PluginUtilsService;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.scripts.exec.AbstractExecScript;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.RunnerType;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
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
// callSuper = false keeps the inherited env out of toString, it can hold credentials
@ToString(callSuper = false)
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractSoda extends AbstractExecScript {
    protected static final ObjectMapper MAPPER = JacksonMapper.ofYaml();
    private static final String REDACTED = "******";
    protected static final String RESULT_FILE = "result.json";
    private static final Set<String> GENERATED_FILES = Set.of(RESULT_FILE, "main.py", "configuration.yml", "checks.yml", "data_source.yml");
    private static final Pattern CONTRACT_FILE = Pattern.compile("contract-\\d+\\.yml");

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
    @JsonProperty("runner")
    protected Property<RunnerType> runnerType;

    // Core has its own non-Property `runner`, kept null so YAML `runner` only binds to runnerType
    @Override
    @JsonIgnore
    public RunnerType getRunner() {
        return null;
    }

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
        description = "Defaults to `sodadata/soda-core:v3.5.2` for `Scan`, or `python:3.12-slim` for `VerifyContract`."
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
        title = "List of python dependencies to add to the python execution process",
        description = "Python dependencies list to setup in the virtualenv, in the same format than requirements.txt. It must at least provides dbt."
    )
    @PluginProperty(group = "advanced")
    protected Property<List<String>> requirements;

    protected abstract String defaultImage();

    // result.json is already in the task output and the generated files hold rendered credentials, a glob must not return them
    protected static Map<String, URI> userOutputFiles(ScriptOutput output) {
        var files = new HashMap<>(output.getOutputFiles());
        files.keySet().removeIf(name -> GENERATED_FILES.contains(name) || CONTRACT_FILE.matcher(name).matches());
        return files;
    }

    protected Map<String, String> finalInputFiles(RunContext runContext, Path workingDirectory) throws IOException, IllegalVariableEvaluationException {
        return this.getInputFiles() != null ? new HashMap<>(PluginUtilsService.transformInputFiles(runContext, this.getInputFiles())) : new HashMap<>();
    }

    public CommandsWrapper start(RunContext runContext) throws Exception {
        var commandsWrapper = this.commands(runContext)
            .withRunnerType(runContext.render(this.getRunnerType()).as(RunnerType.class).orElse(null))
            .withContainerImage(runContext.render(this.getContainerImage()).as(String.class).orElse(this.defaultImage()));
        var rOutputFiles = new ArrayList<>(commandsWrapper.getOutputFiles());
        rOutputFiles.add(RESULT_FILE);
        Path workingDirectory = commandsWrapper.getWorkingDirectory();

        List<String> commands = new ArrayList<>();
        if (this.requirements != null) {
            commands.add(this.virtualEnvCommand(runContext, workingDirectory, runContext.render(this.requirements).asList(String.class)));
            commands.add("./bin/python {{workingDir}}/main.py");
        } else {
            commands.add("python {{workingDir}}/main.py");
        }

        // core writes namespace files first, then these input files, so the generated script always wins
        return commandsWrapper
            .withInputFiles(this.finalInputFiles(runContext, workingDirectory))
            .withOutputFiles(rOutputFiles)
            .addEnv(
                Map.of(
                    "PYTHONUNBUFFERED", "true",
                    "PIP_ROOT_USER_ACTION", "ignore"
                )
            )
            .withInterpreter(this.getInterpreter())
            .withBeforeCommands(this.getBeforeCommands())
            .withBeforeCommandsWithOptions(true)
            .withCommands(Property.ofExpression(JacksonMapper.ofJson().writeValueAsString(commands)));
    }

    @Override
    protected DockerOptions injectDefaults(RunContext runContext, DockerOptions original) {
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

    /**
     * The generated Python scripts report their exit code through a {@code ::{"outputs": {"exitCode": N}}::}
     * stdout directive, parsed by {@link ScriptOutput#getVars()}. If that directive is not captured
     * (e.g. interleaved/garbled stdout), {@code getVars().get("exitCode")} is {@code null}; unboxing
     * that straight into the Output builder's primitive {@code int exitCode} throws an
     * {@link NullPointerException} that masks the real cause. Failing with a clear message here
     * instead surfaces the actual problem (missing exit code directive) rather than a bare NPE.
     */
    protected static int parseExitCode(ScriptOutput output) {
        Integer exitCode = (Integer) output.getVars().get("exitCode");

        if (exitCode == null) {
            throw new IllegalStateException(
                "Soda process did not report an exit code: the `::{\"outputs\": {\"exitCode\": ...}}::` " +
                    "stdout directive was not captured, likely due to interleaved or truncated output."
            );
        }

        return exitCode;
    }
}
