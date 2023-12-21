package com.clickhouse.client.config;

import com.clickhouse.client.ClickHouseConfig;

import javax.net.ssl.*;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;

public class ClickhouseSSLSocketFactory extends SSLSocketFactory {
    private SSLSocketFactory delegate;
    private ClickHouseConfig config;

    public ClickhouseSSLSocketFactory(SSLSocketFactory delegate, ClickHouseConfig config) {
        this.delegate = delegate;
        this.config = config;
    }

    @Override
    public String[] getDefaultCipherSuites() {
        return delegate.getDefaultCipherSuites();
    }

    @Override
    public String[] getSupportedCipherSuites() {
        return delegate.getSupportedCipherSuites();
    }

    private void setServerNameIndication(SSLSocket socket) {
        SSLParameters sslParams = socket.getSSLParameters();
        sslParams.setServerNames(List.of(new SNIHostName(this.config.getServerHostName())));
        socket.setSSLParameters(sslParams);
    }

    private Socket setup(Socket socket) {
        setServerNameIndication((SSLSocket) socket);
        return socket;
    }

    @Override
    public Socket createSocket(Socket socket, final String host, int port, boolean autoClose) throws IOException {
        return setup(delegate.createSocket(socket, host, port, autoClose));
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException, UnknownHostException {
        return setup(delegate.createSocket(host, port));
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress localHost, int localPort) throws IOException, UnknownHostException {
        return setup(delegate.createSocket(host, port, localHost, localPort));
    }

    @Override
    public Socket createSocket(InetAddress host, int port) throws IOException {
        return setup(delegate.createSocket(host, port));
    }

    @Override
    public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort) throws IOException {
        return setup(delegate.createSocket(address, port, localAddress, localPort));
    }
}
