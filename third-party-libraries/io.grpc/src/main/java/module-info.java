module io.grpc {
    exports io.grpc;
    // exports io.grpc.inprocess;
    // exports io.grpc.internal;
    // exports io.grpc.util;
    exports io.grpc.netty.shaded.io.grpc.netty;
    exports io.grpc.netty.shaded.io.netty.channel;
    exports io.grpc.netty.shaded.io.netty.handler;
    exports io.grpc.netty.shaded.io.netty.handler.codec;
    exports io.grpc.netty.shaded.io.netty.handler.codec.http2;
    exports io.grpc.netty.shaded.io.netty.handler.ssl;
    exports io.grpc.netty.shaded.io.netty.handler.ssl.util;

    requires java.logging;
    requires java.naming;
    // requires com.google.errorprone.annotations;
    // requires io.perfmark;

    uses io.grpc.ManagedChannelProvider;
    uses io.grpc.NameResolverProvider;
    uses io.grpc.ServerProvider;
    // uses io.grpc.internal.BinaryLogProvider;
    uses io.grpc.LoadBalancerProvider;
}
