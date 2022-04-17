package com.clickhouse.benchmark;

import static java.time.temporal.ChronoUnit.SECONDS;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.time.Duration;
import java.util.Enumeration;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

@State(Scope.Benchmark)
public class ServerState {
    static String getLocalIpAddress() {
        String localIpAddress = null;

        try {
            for (Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces(); interfaces
                    .hasMoreElements();) {
                NetworkInterface i = interfaces.nextElement();
                if (i.isUp() && !i.isLoopback() && !i.isPointToPoint() && !i.isVirtual()) {
                    for (InterfaceAddress addr : i.getInterfaceAddresses()) {
                        InetAddress inetAddr = addr.getAddress();

                        if (!(inetAddr instanceof Inet4Address)) {
                            continue;
                        }

                        localIpAddress = inetAddr.getHostAddress();
                        break;
                    }
                }

                if (localIpAddress != null) {
                    break;
                }
            }
        } catch (Exception e) {
            // ignore exception
        }

        return localIpAddress != null ? localIpAddress : Constants.DEFAULT_HOST;
    }

    private final String host = System.getProperty("dbHost", Constants.DEFAULT_HOST);
    private final String user = System.getProperty("dbUser", Constants.DEFAULT_USER);
    private final String passwd = System.getProperty("dbPasswd", Constants.DEFAULT_PASSWD);
    private final String db = System.getProperty("dbName", Constants.DEFAULT_DB);

    private final String localIpAddress = getLocalIpAddress();

    private GenericContainer<?> container = null;

    @Setup(Level.Trial)
    public void doSetup() throws Exception {
        if (System.getProperty("dbHost") != null) {
            return;
        }

        String imageTag = System.getProperty("clickhouseVersion");

        if (imageTag == null || (imageTag = imageTag.trim()).isEmpty()) {
            imageTag = "";
        } else {
            imageTag = ":" + imageTag;
        }

        final String imageNameWithTag = "clickhouse/clickhouse-server" + imageTag;

        container = new GenericContainer<>(new ImageFromDockerfile().withDockerfileFromBuilder(builder -> builder
                .from(imageNameWithTag)
                .run("echo '<clickhouse><listen_host>0.0.0.0</listen_host><http_port>8123</http_port>"
                        + "<tcp_port>9000</tcp_port><mysql_port>9004</mysql_port>"
                        + "<postgresql_port>9005</postgresql_port>"
                        + "<interserver_http_port>9009</interserver_http_port>"
                        + "<grpc_port>9100</grpc_port></clickhouse>' > /etc/clickhouse-server/config.d/custom.xml")))
                                .withExposedPorts(Constants.GRPC_PORT, Constants.HTTP_PORT, Constants.MYSQL_PORT,

                                        Constants.NATIVE_PORT)
                                .waitingFor(Wait.forHttp("/ping").forPort(Constants.HTTP_PORT).forStatusCode(200)
                                        .withStartupTimeout(Duration.of(60, SECONDS)));

        container.start();
    }

    @TearDown(Level.Trial)
    public void doTearDown() throws Exception {
        if (container != null) {
            container.stop();
        }
    }

    public String getHost() {
        return container != null ? localIpAddress : host;
    }

    public int getPort(int defaultPort) {
        return container != null ? container.getMappedPort(defaultPort) : defaultPort;
    }

    public String getUser() {
        return container != null ? Constants.DEFAULT_USER : user;
    }

    public String getPassword() {
        return container != null ? Constants.DEFAULT_PASSWD : passwd;
    }

    public String getDatabase() {
        return container != null ? Constants.DEFAULT_DB : db;
    }
}
