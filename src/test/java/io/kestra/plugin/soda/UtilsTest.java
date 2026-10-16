package io.kestra.plugin.soda;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Objects;

import com.google.common.io.CharStreams;

public class UtilsTest {
    // Used with @EnabledIf: the file is absent on fork/Dependabot CI runs where secrets are unavailable.
    public static boolean hasServiceAccount() {
        return UtilsTest.class.getClassLoader().getResource(".gcp-service-account.json") != null;
    }

    // Mirrors the SUFFIX computed in .github/setup-unit.sh: CI runs get their own BigQuery tables so
    // concurrent runs cannot delete each other's data; local runs use the plain name.
    public static String table(String name) {
        String runId = System.getenv("GITHUB_RUN_ID");
        if (runId == null || runId.isEmpty()) {
            return name;
        }
        String attempt = System.getenv("GITHUB_RUN_ATTEMPT");
        return name + "_" + runId + "_" + (attempt == null || attempt.isEmpty() ? "1" : attempt);
    }

    public static String serviceAccount() throws Exception {
        File file = new File(
            Objects.requireNonNull(
                UtilsTest.class.getClassLoader()
                    .getResource(".gcp-service-account.json")
            )
                .toURI()
        );

        return CharStreams.toString(new InputStreamReader(new FileInputStream(file)));
    }
}
