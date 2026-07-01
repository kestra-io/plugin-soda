package io.kestra.plugin.soda;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Fast, dependency-free unit tests for the security-hardening helpers introduced in the
 * 2026-06-30 audit remediation PR: shell-quoting of pip requirements ({@link AbstractSoda})
 * and sensitive-value scrubbing of the rendered configuration ({@link Scan}). Both helpers are
 * {@code private static} pure functions, so they are exercised directly via reflection rather than
 * through the Docker/BigQuery integration path used by {@code ScanTest}.
 */
class SecurityHardeningTest {

    private static String shellQuote(String value) throws Exception {
        Method method = AbstractSoda.class.getDeclaredMethod("shellQuote", String.class);
        method.setAccessible(true);
        return (String) method.invoke(null, value);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> scrub(Map<String, Object> input) throws Exception {
        Method method = Scan.class.getDeclaredMethod("scrubSensitiveValues", Map.class);
        method.setAccessible(true);
        return (Map<String, Object>) method.invoke(null, input);
    }

    // --- shellQuote -------------------------------------------------------------------------

    @Test
    void shellQuote_wrapsPlainRequirement() throws Exception {
        assertThat(shellQuote("soda-core-postgres"), is("'soda-core-postgres'"));
    }

    @Test
    void shellQuote_preservesVersionOperatorsLiterally() throws Exception {
        // `>` and `<` used to be interpreted as shell redirection when interpolated bare.
        assertThat(shellQuote("soda-core[postgres]>=3.0,<4.0"), is("'soda-core[postgres]>=3.0,<4.0'"));
    }

    @Test
    void shellQuote_neutralizesShellMetacharacters() throws Exception {
        // Command-injection attempt: the `;` and everything after must stay inside the quotes.
        String quoted = shellQuote("pkg; rm -rf /");
        assertThat(quoted, is("'pkg; rm -rf /'"));
        // Nothing escapes the single-quoted span, so no bare metacharacter is exposed to the shell.
        assertThat(quoted.startsWith("'"), is(true));
        assertThat(quoted.endsWith("'"), is(true));
    }

    @Test
    void shellQuote_escapesEmbeddedSingleQuote() throws Exception {
        // The one character that cannot live inside single quotes is escaped as '\'' .
        assertThat(shellQuote("a'b"), is("'a'\\''b'"));
    }

    @Test
    void shellQuote_preservesEnvironmentMarkers() throws Exception {
        assertThat(
            shellQuote("pkg; python_version >= \"3.8\""),
            is("'pkg; python_version >= \"3.8\"'")
        );
    }

    // --- scrubSensitiveValues ---------------------------------------------------------------

    @Test
    void scrub_redactsTopLevelSensitiveKeys() throws Exception {
        Map<String, Object> input = new LinkedHashMap<>();
        input.put("password", "hunter2");
        input.put("host", "db.internal");

        Map<String, Object> result = scrub(input);

        assertThat(result.get("password"), is("******"));
        assertThat(result.get("host"), is("db.internal"));
    }

    @Test
    void scrub_recursesIntoNestedMaps() throws Exception {
        Map<String, Object> connection = new LinkedHashMap<>();
        connection.put("password", "s3cret");
        connection.put("port", 5432);

        Map<String, Object> input = new LinkedHashMap<>();
        input.put("connection", connection);

        Map<String, Object> result = scrub(input);

        @SuppressWarnings("unchecked")
        Map<String, Object> scrubbedConnection = (Map<String, Object>) result.get("connection");
        assertThat(scrubbedConnection.get("password"), is("******"));
        // Non-sensitive leaf values keep their original value and type.
        assertThat(scrubbedConnection.get("port"), is(5432));
    }

    @Test
    void scrub_redactsSecretKeyVariants() throws Exception {
        Map<String, Object> input = new LinkedHashMap<>();
        input.put("api_key", "abc");
        input.put("apiKey", "abc");
        input.put("token", "xyz");
        input.put("account_info_json", "{...}");
        input.put("client_secret", "shh");
        input.put("type", "postgres");

        Map<String, Object> result = scrub(input);

        assertThat(result.get("api_key"), is("******"));
        assertThat(result.get("apiKey"), is("******"));
        assertThat(result.get("token"), is("******"));
        assertThat(result.get("account_info_json"), is("******"));
        assertThat(result.get("client_secret"), is("******"));
        assertThat(result.get("type"), is("postgres"));
    }

    @Test
    void scrub_recursesIntoLists() throws Exception {
        // A non-sensitive key whose value is a list of maps: secrets nested in list elements
        // must still be redacted (the recursion used to stop at Map values only).
        Map<String, Object> node1 = new LinkedHashMap<>();
        node1.put("host", "db1");
        node1.put("password", "s1");
        Map<String, Object> node2 = new LinkedHashMap<>();
        node2.put("host", "db2");
        node2.put("token", "t2");

        Map<String, Object> input = new LinkedHashMap<>();
        input.put("nodes", new ArrayList<>(List.of(node1, node2)));

        Map<String, Object> result = scrub(input);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> nodes = (List<Map<String, Object>>) result.get("nodes");
        assertThat(nodes, hasSize(2));
        assertThat(nodes.get(0).get("password"), is("******"));
        assertThat(nodes.get(0).get("host"), is("db1"));
        assertThat(nodes.get(1).get("token"), is("******"));
        assertThat(nodes.get(1).get("host"), is("db2"));
    }

    @Test
    void scrub_redactsAnyKeyContainingSensitiveSubstring() throws Exception {
        // Deliberate over-redaction: matching is by substring, so any key merely containing a
        // sensitive fragment is masked rather than risk leaking a secret.
        Map<String, Object> input = new LinkedHashMap<>();
        input.put("keyfile", "/path/to/sa.json");
        input.put("keyspace", "analytics");
        input.put("sortkey", "created_at");
        input.put("author", "alice");

        Map<String, Object> result = scrub(input);

        assertThat(result.get("keyfile"), is("******"));
        assertThat(result.get("keyspace"), is("******"));
        assertThat(result.get("sortkey"), is("******"));
        assertThat(result.get("author"), is("******"));
    }
}
