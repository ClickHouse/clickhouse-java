package com.clickhouse.client;

import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import com.clickhouse.client.ClickHouseRequest.Mutation;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseOutputStream;

public class ClickHouseClientTest {
    @Test(groups = { "unit" })
    public void testGetAsyncRequestOutputStream() throws IOException {
        ClickHouseConfig config = new ClickHouseConfig();
        for (int i = 0; i < 256; i++) {
            ByteArrayOutputStream bas = new ByteArrayOutputStream();
            try (ClickHouseOutputStream chOut = ClickHouseClient.getAsyncRequestOutputStream(config, bas, null)) {
                chOut.write(i);
            }
            Assert.assertEquals(bas.toByteArray(), new byte[] { (byte) i });
        }
    }

    @Test(groups = { "unit" })
    public void testGetRequestOutputStream() throws IOException {
        ClickHouseConfig config = new ClickHouseConfig();
        for (int i = 0; i < 256; i++) {
            ByteArrayOutputStream bas = new ByteArrayOutputStream();
            try (ClickHouseOutputStream chOut = ClickHouseClient.getRequestOutputStream(config, bas, null)) {
                chOut.write(i);
            }
            Assert.assertEquals(bas.toByteArray(), new byte[] { (byte) i });
        }
    }

    @Test(groups = { "unit" })
    public void testQuery() throws ExecutionException, InterruptedException {
        ClickHouseClient client = ClickHouseClient.builder().build();
        Assert.assertNotNull(client);
        ClickHouseRequest<?> req = client.read(ClickHouseNode.builder().build());
        Assert.assertNotNull(req);
        Assert.assertNull(req.config);
        Assert.assertNotNull(req.getConfig());
        Assert.assertNotNull(req.config);
        Assert.assertEquals(req.getClient(), client);
        Assert.assertEquals(req.getFormat(), client.getConfig().getFormat());
        Assert.assertNull(req.sql);
        Assert.assertNull(req.query("select 1").execute().get());
    }

    @Test(groups = { "unit" })
    public void testMutation() throws ExecutionException, InterruptedException {
        ClickHouseClient client = ClickHouseClient.builder().build();
        Assert.assertNotNull(client);
        Mutation req = client.read(ClickHouseNode.builder().build()).write();
        Assert.assertNotNull(req);
        Assert.assertNull(req.config);
        Assert.assertNotNull(req.getConfig());
        Assert.assertNotNull(req.config);
        Assert.assertEquals(req.getClient(), client);
        Assert.assertEquals(req.getFormat(), client.getConfig().getFormat());
        Assert.assertNull(req.sql);
        Assert.assertNull(req.table("my_table").format(ClickHouseFormat.RowBinary).execute().get());
    }

    @Test(groups = { "unit" })
    public void testDefaultServiceFallback() {
        Assert.assertNotNull(ClickHouseDnsResolver.getInstance());
        Assert.assertNotNull(ClickHouseRequestManager.getInstance());
    }

    @Test(groups = { "unit" })
    public void testServicesLoadFromNamedModuleProvider() throws Exception {
        if (getJavaVersion() < 11) {
            throw new SkipException("Requires Java 11 or later");
        }

        String baseDir = System.getProperty("basedir", ".");
        Path clientClasses = Paths.get(baseDir, "target", "classes");
        Path dataClasses = Paths.get(baseDir, "..", "clickhouse-data", "target", "classes").normalize();
        if (!Files.isRegularFile(clientClasses.resolve("META-INF/versions/11/module-info.class"))
                || !Files.isRegularFile(dataClasses.resolve("META-INF/versions/11/module-info.class"))) {
            throw new SkipException("Multi-release module descriptors were not compiled");
        }

        Path tempDir = Files.createTempDirectory("clickhouse-client-module-path-");
        try {
            Path clientJar = tempDir.resolve("clickhouse-client.jar");
            Path dataJar = tempDir.resolve("clickhouse-data.jar");
            Path providerClasses = tempDir.resolve("provider-classes");
            createModuleJar(clientClasses, clientJar);
            createModuleJar(dataClasses, dataJar);

            Path fixtures = Paths.get(baseDir, "src", "test", "resources", "jpms-service-provider");
            runProcess(Arrays.asList(javaTool("javac"), "--release", "11", "--module-path",
                    clientJar + File.pathSeparator + dataJar, "-d", providerClasses.toString(),
                    fixtures.resolve("module-info.java").toString(), fixtures.resolve("test/provider/TestRequestManager.java").toString(),
                    fixtures.resolve("test/provider/TestDnsResolver.java").toString(),
                    fixtures.resolve("test/provider/Main.java").toString()));

            runProcess(Arrays.asList(javaTool("java"), "--module-path",
                    clientJar + File.pathSeparator + dataJar + File.pathSeparator + providerClasses, "-m",
                    "test.clickhouse.client.provider/test.provider.Main"));
        } finally {
            deleteRecursively(tempDir);
        }
    }

    private static int getJavaVersion() {
        String version = System.getProperty("java.specification.version");
        return Integer.parseInt(version.startsWith("1.") ? version.substring(2) : version);
    }

    private static String javaTool(String name) {
        String extension = System.getProperty("os.name").startsWith("Windows") ? ".exe" : "";
        return new File(new File(System.getProperty("java.home"), "bin"), name + extension).getAbsolutePath();
    }

    private static void runProcess(List<String> command) throws IOException, InterruptedException {
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        for (int read; (read = process.getInputStream().read(buffer)) != -1;) {
            output.write(buffer, 0, read);
        }

        int exitCode = process.waitFor();
        Assert.assertEquals(exitCode, 0, "Command failed: " + command + "\n"
                + new String(output.toByteArray(), StandardCharsets.UTF_8));
    }

    private static void createModuleJar(final Path classesDir, Path moduleJar) throws IOException {
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().putValue("Multi-Release", "true");

        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(moduleJar), manifest)) {
            Files.walkFileTree(classesDir, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) throws IOException {
                    String entryName = classesDir.relativize(file).toString().replace(File.separatorChar, '/');
                    if (!"META-INF/MANIFEST.MF".equalsIgnoreCase(entryName)) {
                        output.putNextEntry(new JarEntry(entryName));
                        Files.copy(file, output);
                        output.closeEntry();
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        }
    }

    private static void deleteRecursively(Path path) throws IOException {
        Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path directory, IOException exception) throws IOException {
                if (exception != null) {
                    throw exception;
                }
                Files.delete(directory);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
