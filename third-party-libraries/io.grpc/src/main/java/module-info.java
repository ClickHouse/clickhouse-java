module io.grpc {
    exports io.grpc;
    // exports io.grpc.inprocess;
    // exports io.grpc.internal;
    // exports io.grpc.util;

    requires java.logging;
    requires java.naming;
    // requires com.google.errorprone.annotations;
    // requires com.lmax.disruptor;
    // requires io.perfmark;

    uses io.grpc.ManagedChannelProvider;
    uses io.grpc.NameResolverProvider;
    uses io.grpc.ServerProvider;
    // uses io.grpc.internal.BinaryLogProvider;
    uses io.grpc.LoadBalancerProvider;
}
