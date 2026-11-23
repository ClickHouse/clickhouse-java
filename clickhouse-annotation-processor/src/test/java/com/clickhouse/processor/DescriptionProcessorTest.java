package com.clickhouse.processor;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class DescriptionProcessorTest {

    private static final String DESCRIPTION_SOURCE = "package com.clickhouse.client.api.internal;\n"
        + "@java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.SOURCE)\n"
        + "@java.lang.annotation.Target(java.lang.annotation.ElementType.FIELD)\n"
        + "public @interface ConfigDescription { String value(); }\n";

    private Path outputDir;

    private DiagnosticCollector<JavaFileObject> diagnostics;

    @BeforeMethod
    public void setUp() throws IOException {
        outputDir = Files.createTempDirectory("description-processor");
        diagnostics = new DiagnosticCollector<>();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws IOException {
        try (Stream<Path> files = Files.walk(outputDir)) {
            for (Path file : files.sorted(Comparator.reverseOrder()).collect(Collectors.toList())) {
                Files.delete(file);
            }
        }
    }

    @Test
    public void testGeneratesDescriptions() throws Exception {
        assertTrue(compile("p.Settings",
            "package p;\n"
                + "import com.clickhouse.client.api.internal.ConfigDescription;\n"
                + "public enum Settings {\n"
                + "    @ConfigDescription(\"Plain text.\") PLAIN(\"plain\"),\n"
                + "    @Deprecated @ConfigDescription(\"Quote \\\" and backslash \\\\ and \\\\u0022.\")\n"
                + "        QUOTED(\"quoted\"),\n"
                + "    @ConfigDescription(\"Line 1\\nline 2\\ttab \\u00b5s \\u20ac.\") ESCAPED(\"escaped\") {\n"
                + "        @Override public String key() { return \"other\"; }\n"
                + "    };\n"
                + "    private final String key;\n"
                + "    Settings(String key) { this.key = key; }\n"
                + "    public String key() { return key; }\n"
                + "}\n"), diagnostics.getDiagnostics().toString());
        assertTrue(diagnostics.getDiagnostics().isEmpty(), diagnostics.getDiagnostics().toString());

        try (URLClassLoader loader = new URLClassLoader(new URL[] {outputDir.toUri().toURL()})) {
            Class<?> generated = loader.loadClass("p.SettingsDescriptions");
            assertTrue(Modifier.isPublic(generated.getModifiers()));
            assertTrue(Modifier.isFinal(generated.getModifiers()));

            Map<?, ?> descriptions = (Map<?, ?>) generated.getMethod("getDescriptions").invoke(null);
            Map<String, String> byName = new LinkedHashMap<>();
            descriptions.forEach((constant, description) -> byName.put(((Enum<?>) constant).name(), (String) description));
            Map<String, String> expected = new LinkedHashMap<>();
            expected.put("PLAIN", "Plain text.");
            expected.put("QUOTED", "Quote \" and backslash \\ and \\u0022.");
            expected.put("ESCAPED", "Line 1\nline 2\ttab \u00b5s \u20ac.");
            assertEquals(new ArrayList<>(byName.entrySet()), new ArrayList<>(expected.entrySet()));
            expectThrows(UnsupportedOperationException.class, () -> descriptions.clear());
        }
    }

    @Test
    public void testPackagePrivateEnum() throws Exception {
        assertTrue(compile("p.Hidden",
            "package p;\n"
                + "enum Hidden { @com.clickhouse.client.api.internal.ConfigDescription(\"Only constant.\") ONLY }\n"),
            diagnostics.getDiagnostics().toString());

        try (URLClassLoader loader = new URLClassLoader(new URL[] {outputDir.toUri().toURL()})) {
            Class<?> generated = loader.loadClass("p.HiddenDescriptions");
            assertFalse(Modifier.isPublic(generated.getModifiers()));
        }
    }

    @Test
    public void testIgnoresEnumWithoutDescriptions() throws Exception {
        assertTrue(compile("p.Plain", "package p;\npublic enum Plain { A, B }\n"),
            diagnostics.getDiagnostics().toString());
        assertTrue(Files.exists(outputDir.resolve("p/Plain.class")));
        assertFalse(Files.exists(outputDir.resolve("p/PlainDescriptions.java")));
        assertFalse(Files.exists(outputDir.resolve("p/PlainDescriptions.class")));
    }

    @DataProvider(name = "invalidUses")
    public Object[][] invalidUses() {
        return new Object[][] {
            {"package p;\nimport com.clickhouse.client.api.internal.ConfigDescription;\n"
                + "public enum Invalid { @ConfigDescription(\"First.\") FIRST, SECOND }\n",
                "Enum constant Invalid.SECOND has no @ConfigDescription"},
            {"package p;\nimport com.clickhouse.client.api.internal.ConfigDescription;\n"
                + "public enum Invalid { @ConfigDescription(\"First.\") FIRST, @ConfigDescription(\" \") SECOND }\n",
                "Enum constant Invalid.SECOND has a blank @ConfigDescription"},
            {"package p;\nimport com.clickhouse.client.api.internal.ConfigDescription;\n"
                + "public class Invalid { @ConfigDescription(\"Field.\") static final String FIELD = \"field\"; }\n",
                "@ConfigDescription is allowed only on enum constants"},
            {"package p;\nimport com.clickhouse.client.api.internal.ConfigDescription;\n"
                + "public class Invalid { enum Nested { @ConfigDescription(\"Nested.\") NESTED } }\n",
                "@ConfigDescription is allowed only on constants of top-level enums"},
        };
    }

    @Test(dataProvider = "invalidUses")
    public void testRejectsInvalidUse(String source, String expectedError) throws Exception {
        assertFalse(compile("p.Invalid", source));
        List<String> errors = diagnostics.getDiagnostics().stream()
            .filter(d -> d.getKind() == Diagnostic.Kind.ERROR)
            .map(d -> d.getMessage(null))
            .collect(Collectors.toList());
        assertEquals(errors, Collections.singletonList(expectedError));
        assertFalse(Files.exists(outputDir.resolve("p/InvalidDescriptions.java")));
    }

    @Test
    public void testToJavaLiteral() {
        assertEquals(DescriptionProcessor.toJavaLiteral("a\"b\\c\r\n\u0000\u007f\u00e4"),
            "\"a\\\"b\\\\c\\015\\012\\000\\177\\u00e4\"");
    }

    /**
     * Compiles the source together with the {@code ConfigDescription} annotation, with only
     * {@link DescriptionProcessor}.
     * Writes the generated sources and the classes to {@link #outputDir}.
     */
    private boolean compile(String className, String source) throws IOException {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        assertNotNull(compiler, "Tests need a JDK");
        try (StandardJavaFileManager fileManager = compiler.getStandardFileManager(diagnostics, null,
                StandardCharsets.UTF_8)) {
            List<File> output = Collections.singletonList(outputDir.toFile());
            fileManager.setLocation(StandardLocation.CLASS_OUTPUT, output);
            fileManager.setLocation(StandardLocation.SOURCE_OUTPUT, output);
            JavaCompiler.CompilationTask task = compiler.getTask(null, fileManager, diagnostics,
                Arrays.asList("-Xlint:deprecation,rawtypes,unchecked", "-implicit:none"), null,
                Arrays.asList(source("com.clickhouse.client.api.internal.ConfigDescription", DESCRIPTION_SOURCE),
                    source(className, source)));
            task.setProcessors(Collections.singletonList(new DescriptionProcessor()));
            return task.call();
        }
    }

    private static JavaFileObject source(String className, String code) {
        return new SimpleJavaFileObject(URI.create("string:///" + className.replace('.', '/') + ".java"),
                JavaFileObject.Kind.SOURCE) {
            @Override
            public CharSequence getCharContent(boolean ignoreEncodingErrors) {
                return code;
            }
        };
    }
}
