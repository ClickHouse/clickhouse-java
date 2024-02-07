package com.clickhouse.client;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.commons.compress.utils.IOUtils;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.shaded.org.apache.commons.lang3.StringUtils;
import org.testcontainers.utility.MountableFile;

import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;

public class KdcServerForTest {

    private static final Logger log = LoggerFactory.getLogger(KdcServerForTest.class);

    private static final int KDC_PORT = 88;
    private static final int ADMIN_PORT = 749;
    private static final String DOCKER_FILE = "containers/kdc-server/Dockerfile";
    private static final String CLICKHOUSE_SNAME = "HTTP/clickhouse-server.example.com@EXAMPLE.COM";
    private static final String BOB_KEYTAB_NAME = "bob.keytab";

    private final GenericContainer<?> kdcContainer;
    private final GenericContainer<?> clickhouseContainer;
    private final File tmpDir;
    private String bobJaasConfPath;
    private String krb5ConfPath;

    private static KdcServerForTest instance;

    public static KdcServerForTest getInstance() {
        if (instance == null) {
            synchronized(KdcServerForTest.class) {
                if (instance == null) {
                    instance = new KdcServerForTest(ClickHouseServerForTest.getClickHouseContainer());
                }
            }
        }
        return instance;
    }

    private KdcServerForTest(GenericContainer<?> clickhouseContainer) {
        if (clickhouseContainer == null) {
            throw new IllegalArgumentException("Clickhouse server container can not be null");
        }
        this.clickhouseContainer = clickhouseContainer;
        this.kdcContainer = buildKdcContainer();
        tmpDir = new File(System.getProperty("java.io.tmpdir"), "test-" + UUID.randomUUID());
    }

    private static GenericContainer<?> buildKdcContainer() {
        String dockerFile = getClassLoader().getResource(DOCKER_FILE).getFile();
        return new GenericContainer<>(
            new ImageFromDockerfile().withDockerfile(new File(dockerFile).toPath()))
            .withExposedPorts(KDC_PORT, ADMIN_PORT);
    }

    public void beforeSuite() {
        if (!tmpDir.exists()) {
            log.info("Creating tmp directory " + tmpDir.getAbsolutePath());
            tmpDir.mkdir();
            tmpDir.deleteOnExit();
        }

        if (kdcContainer != null) {
            if (kdcContainer.isRunning()) {
                return;
            }

            if (!clickhouseContainer.isRunning()) {
                throw new IllegalStateException("ClickHouse server is not initialized");
            }

            try {
                log.info("Starting KDC container...");
                kdcContainer.start();
                executeCmd("kadmin.local add_principal -pw bob bob@EXAMPLE.COM");
                executeCmd("kadmin.local ktadd -k /etc/bob.keytab -norandkey bob@EXAMPLE.COM");
                executeCmd("kadmin.local add_principal -randkey " + CLICKHOUSE_SNAME);
                executeCmd("kadmin.local ktadd -k /etc/ch-service.keytab -norandkey " + CLICKHOUSE_SNAME);

                File bobKeyTab = new File(tmpDir, BOB_KEYTAB_NAME);
                kdcContainer.copyFileFromContainer("/etc/bob.keytab", bobKeyTab.getAbsolutePath());

                File chServiceKeyTab = new File(tmpDir, "ch.keytab");
                kdcContainer.copyFileFromContainer("/etc/ch-service.keytab", chServiceKeyTab.getAbsolutePath());
                clickhouseContainer.copyFileToContainer(
                        MountableFile.forHostPath(chServiceKeyTab.getAbsolutePath(), 644), "/etc/krb5.keytab");

                bobJaasConfPath = createBobJaasConf();
                krb5ConfPath = createKrb5Conf();
            } catch (Exception e) {
                throw new IllegalArgumentException("Can not initialize kdc server", e);
            }
        }
        log.info("KDC container started");
    }

    public String getBobJaasConf() {
        return bobJaasConfPath;
    }

    public String getKrb5Conf() {
        return krb5ConfPath;
    }

    public void afterSuite() {
        if (kdcContainer != null) {
            kdcContainer.stop();
            kdcContainer.close();
        }
    }

    private void executeCmd(String cmd) throws UnsupportedOperationException, IOException, InterruptedException {
        ExecResult result = kdcContainer.execInContainer(cmd.split(" "));
        validate(result, cmd);
    }

    private void validate(ExecResult result, String cmd) {
        if (result.getExitCode() != 0) {
            String reason = result.getStdout();
            if (StringUtils.isNoneBlank(result.getStderr())) {
                reason = result.getStderr();
            }
            throw new RuntimeException(
                    "Command [" + cmd + "] failed with code " + result.getExitCode() + " and reason " + reason);
        }
        if (StringUtils.isNotBlank(result.getStdout())) {
            log.info("[" + cmd + "]: " + result.getStdout());
        }
    }

    private String createBobJaasConf() throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("PRINCIPAL", "bob@EXAMPLE.COM");
        params.put("KEYTAB", new File(tmpDir, BOB_KEYTAB_NAME).getAbsolutePath());
        return prepareConfigFile("client_jaas.conf", "bob_jaas.conf", params);
    }

    private String createKrb5Conf() throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("KDC_PORT", "" + kdcContainer.getMappedPort(KDC_PORT));
        params.put("ADMIN_PORT", "" + kdcContainer.getMappedPort(ADMIN_PORT));
        return prepareConfigFile("client_krb5.conf", "krb5.conf", params);
    }

    private String prepareConfigFile(String templateFileName, String outputFileName, Map<String, String> params) throws IOException {
        try (InputStream inputStream = KdcServerForTest.class.getClassLoader().getResourceAsStream(templateFileName)) {
            String templateFile = new String(IOUtils.toByteArray(inputStream));
            String content = templateFile;
            for (Entry<String, String> param : params.entrySet()) {
                content = content.replaceAll(Pattern.quote("${" + param.getKey() + "}"), param.getValue());
            }
            File outputFile = new File(tmpDir, outputFileName);
            try (FileWriter writer = new FileWriter(outputFile)) {
                writer.write(content);
            }
            return outputFile.getAbsolutePath();
        }
    }

    private static ClassLoader getClassLoader() {
        return KdcServerForTest.class.getClassLoader();
    }
}