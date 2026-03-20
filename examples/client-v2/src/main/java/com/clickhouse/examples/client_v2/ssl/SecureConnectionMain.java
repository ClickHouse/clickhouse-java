package com.clickhouse.examples.client_v2.ssl;

import com.clickhouse.client.api.Client;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.TypeConversionException;

import java.util.Locale;
import java.util.concurrent.Callable;

@Command(
        name = "secure-connection",
        mixinStandardHelpOptions = true,
        description = "Connects to ClickHouse over HTTPS using the provided SSL mode."
)
public class SecureConnectionMain implements Callable<Integer> {

    private static final String SSL_MODE_OPTION = "sslmode";

    enum SslMode {
        NONE,
        STRICT
    }

    static class SslModeConverter implements CommandLine.ITypeConverter<SslMode> {
        @Override
        public SslMode convert(String value) {
            try {
                return SslMode.valueOf(value.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new TypeConversionException("ssl_mode must be one of: none, strict");
            }
        }
    }

    @Option(names = "--hostname", required = true, description = "ClickHouse server hostname.")
    private String hostname;

    @Option(names = "--username", required = true, description = "Username used for authentication.")
    private String username;

    @Option(names = "--port", required = true, description = "HTTPS port of the ClickHouse server.")
    private int port;

    @Option(
            names = {"--ssl_mode", "--ssl-mode"},
            required = true,
            converter = SslModeConverter.class,
            description = "SSL verification mode. Valid values: ${COMPLETION-CANDIDATES}."
    )
    private SslMode sslMode;

    @Override
    public Integer call() {
        try (Client client = new Client.Builder()
                .addEndpoint("https://" + hostname + ":" + port)
                .setUsername(username)
                .setOption(SSL_MODE_OPTION, sslMode.name().toLowerCase(Locale.ROOT))
                .build()) {
            boolean isReachable = client.ping();
            System.out.printf(
                    "Connection to https://%s:%d as %s with ssl_mode=%s: %s%n",
                    hostname,
                    port,
                    username,
                    sslMode.name().toLowerCase(Locale.ROOT),
                    isReachable ? "OK" : "FAILED");
            return isReachable ? CommandLine.ExitCode.OK : CommandLine.ExitCode.SOFTWARE;
        }
    }

    public static void main(String... args) {
        System.exit(new CommandLine(new SecureConnectionMain()).execute(args));
    }
}
