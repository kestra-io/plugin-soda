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
