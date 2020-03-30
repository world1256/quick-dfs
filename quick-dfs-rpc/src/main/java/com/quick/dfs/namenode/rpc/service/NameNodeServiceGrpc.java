package com.quick.dfs.namenode.rpc.service;

import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;

@javax.annotation.Generated("by gRPC proto compiler")
public class NameNodeServiceGrpc {

  private NameNodeServiceGrpc() {}

  public static final String SERVICE_NAME = "com.quick.dfs.namenode.rpc.NameNodeService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.quick.dfs.namenode.rpc.model.RegisterRequest,
      com.quick.dfs.namenode.rpc.model.RegisterResponse> METHOD_REGISTER =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "com.quick.dfs.namenode.rpc.NameNodeService", "register"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.quick.dfs.namenode.rpc.model.RegisterRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.quick.dfs.namenode.rpc.model.RegisterResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.quick.dfs.namenode.rpc.model.HeartbeatRequest,
      com.quick.dfs.namenode.rpc.model.HeartbeatResponse> METHOD_HEARTBEAT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "com.quick.dfs.namenode.rpc.NameNodeService", "heartbeat"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.quick.dfs.namenode.rpc.model.HeartbeatRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.quick.dfs.namenode.rpc.model.HeartbeatResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.quick.dfs.namenode.rpc.model.MkDirRequest,
      com.quick.dfs.namenode.rpc.model.MkDirResponse> METHOD_MK_DIR =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "com.quick.dfs.namenode.rpc.NameNodeService", "mkDir"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.quick.dfs.namenode.rpc.model.MkDirRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.quick.dfs.namenode.rpc.model.MkDirResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.quick.dfs.namenode.rpc.model.ShutdownRequest,
      com.quick.dfs.namenode.rpc.model.ShutdownResponse> METHOD_SHUTDOWN =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "com.quick.dfs.namenode.rpc.NameNodeService", "shutdown"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.quick.dfs.namenode.rpc.model.ShutdownRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.quick.dfs.namenode.rpc.model.ShutdownResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.quick.dfs.namenode.rpc.model.FetchEditLogRequest,
      com.quick.dfs.namenode.rpc.model.FetchEditLogResponse> METHOD_FETCH_EDIT_LOG =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "com.quick.dfs.namenode.rpc.NameNodeService", "fetchEditLog"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.quick.dfs.namenode.rpc.model.FetchEditLogRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.quick.dfs.namenode.rpc.model.FetchEditLogResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.quick.dfs.namenode.rpc.model.UpdateCheckpointTxidRequest,
      com.quick.dfs.namenode.rpc.model.UpdateCheckpointTxidResponse> METHOD_UPDATE_CHECKPOINT_TXID =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "com.quick.dfs.namenode.rpc.NameNodeService", "updateCheckpointTxid"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.quick.dfs.namenode.rpc.model.UpdateCheckpointTxidRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.quick.dfs.namenode.rpc.model.UpdateCheckpointTxidResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.quick.dfs.namenode.rpc.model.CreateFileRequest,
      com.quick.dfs.namenode.rpc.model.CreateFileResponse> METHOD_CREATE_FILE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "com.quick.dfs.namenode.rpc.NameNodeService", "createFile"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.quick.dfs.namenode.rpc.model.CreateFileRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.quick.dfs.namenode.rpc.model.CreateFileResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.quick.dfs.namenode.rpc.model.AllocateDataNodesRequest,
      com.quick.dfs.namenode.rpc.model.AllocateDataNodesResponse> METHOD_ALLOCATE_DATA_NODES =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "com.quick.dfs.namenode.rpc.NameNodeService", "allocateDataNodes"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.quick.dfs.namenode.rpc.model.AllocateDataNodesRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.quick.dfs.namenode.rpc.model.AllocateDataNodesResponse.getDefaultInstance()));

  public static NameNodeServiceStub newStub(io.grpc.Channel channel) {
    return new NameNodeServiceStub(channel);
  }

  public static NameNodeServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new NameNodeServiceBlockingStub(channel);
  }

  public static NameNodeServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new NameNodeServiceFutureStub(channel);
  }

  public static interface NameNodeService {

    public void register(com.quick.dfs.namenode.rpc.model.RegisterRequest request,
                         io.grpc.stub.StreamObserver<com.quick.dfs.namenode.rpc.model.RegisterResponse> responseObserver);

    public void heartbeat(com.quick.dfs.namenode.rpc.model.HeartbeatRequest request,
                          io.grpc.stub.StreamObserver<com.quick.dfs.namenode.rpc.model.HeartbeatResponse> responseObserver);

    public void mkDir(com.quick.dfs.namenode.rpc.model.MkDirRequest request,
                      io.grpc.stub.StreamObserver<com.quick.dfs.namenode.rpc.model.MkDirResponse> responseObserver);

    public void shutdown(com.quick.dfs.namenode.rpc.model.ShutdownRequest request,
                         io.grpc.stub.StreamObserver<com.quick.dfs.namenode.rpc.model.ShutdownResponse> responseObserver);

    public void fetchEditLog(com.quick.dfs.namenode.rpc.model.FetchEditLogRequest request,
                             io.grpc.stub.StreamObserver<com.quick.dfs.namenode.rpc.model.FetchEditLogResponse> responseObserver);

    public void updateCheckpointTxid(com.quick.dfs.namenode.rpc.model.UpdateCheckpointTxidRequest request,
                                     io.grpc.stub.StreamObserver<com.quick.dfs.namenode.rpc.model.UpdateCheckpointTxidResponse> responseObserver);

    public void createFile(com.quick.dfs.namenode.rpc.model.CreateFileRequest request,
                           io.grpc.stub.StreamObserver<com.quick.dfs.namenode.rpc.model.CreateFileResponse> responseObserver);

    public void allocateDataNodes(com.quick.dfs.namenode.rpc.model.AllocateDataNodesRequest request,
                                  io.grpc.stub.StreamObserver<com.quick.dfs.namenode.rpc.model.AllocateDataNodesResponse> responseObserver);
  }

  public static interface NameNodeServiceBlockingClient {

    public com.quick.dfs.namenode.rpc.model.RegisterResponse register(com.quick.dfs.namenode.rpc.model.RegisterRequest request);

    public com.quick.dfs.namenode.rpc.model.HeartbeatResponse heartbeat(com.quick.dfs.namenode.rpc.model.HeartbeatRequest request);

    public com.quick.dfs.namenode.rpc.model.MkDirResponse mkDir(com.quick.dfs.namenode.rpc.model.MkDirRequest request);

    public com.quick.dfs.namenode.rpc.model.ShutdownResponse shutdown(com.quick.dfs.namenode.rpc.model.ShutdownRequest request);

    public com.quick.dfs.namenode.rpc.model.FetchEditLogResponse fetchEditLog(com.quick.dfs.namenode.rpc.model.FetchEditLogRequest request);

    public com.quick.dfs.namenode.rpc.model.UpdateCheckpointTxidResponse updateCheckpointTxid(com.quick.dfs.namenode.rpc.model.UpdateCheckpointTxidRequest request);

    public com.quick.dfs.namenode.rpc.model.CreateFileResponse createFile(com.quick.dfs.namenode.rpc.model.CreateFileRequest request);

    public com.quick.dfs.namenode.rpc.model.AllocateDataNodesResponse allocateDataNodes(com.quick.dfs.namenode.rpc.model.AllocateDataNodesRequest request);
  }

  public static interface NameNodeServiceFutureClient {

    public com.google.common.util.concurrent.ListenableFuture<com.quick.dfs.namenode.rpc.model.RegisterResponse> register(
            com.quick.dfs.namenode.rpc.model.RegisterRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.quick.dfs.namenode.rpc.model.HeartbeatResponse> heartbeat(
            com.quick.dfs.namenode.rpc.model.HeartbeatRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.quick.dfs.namenode.rpc.model.MkDirResponse> mkDir(
            com.quick.dfs.namenode.rpc.model.MkDirRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.quick.dfs.namenode.rpc.model.ShutdownResponse> shutdown(
            com.quick.dfs.namenode.rpc.model.ShutdownRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.quick.dfs.namenode.rpc.model.FetchEditLogResponse> fetchEditLog(
            com.quick.dfs.namenode.rpc.model.FetchEditLogRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.quick.dfs.namenode.rpc.model.UpdateCheckpointTxidResponse> updateCheckpointTxid(
            com.quick.dfs.namenode.rpc.model.UpdateCheckpointTxidRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.quick.dfs.namenode.rpc.model.CreateFileResponse> createFile(
            com.quick.dfs.namenode.rpc.model.CreateFileRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.quick.dfs.namenode.rpc.model.AllocateDataNodesResponse> allocateDataNodes(
            com.quick.dfs.namenode.rpc.model.AllocateDataNodesRequest request);
  }

  public static class NameNodeServiceStub extends io.grpc.stub.AbstractStub<NameNodeServiceStub>
      implements NameNodeService {
    private NameNodeServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private NameNodeServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected NameNodeServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new NameNodeServiceStub(channel, callOptions);
    }

    @Override
    public void register(com.quick.dfs.namenode.rpc.model.RegisterRequest request,
        io.grpc.stub.StreamObserver<com.quick.dfs.namenode.rpc.model.RegisterResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_REGISTER, getCallOptions()), request, responseObserver);
    }

    @Override
    public void heartbeat(com.quick.dfs.namenode.rpc.model.HeartbeatRequest request,
        io.grpc.stub.StreamObserver<com.quick.dfs.namenode.rpc.model.HeartbeatResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_HEARTBEAT, getCallOptions()), request, responseObserver);
    }

    @Override
    public void mkDir(com.quick.dfs.namenode.rpc.model.MkDirRequest request,
        io.grpc.stub.StreamObserver<com.quick.dfs.namenode.rpc.model.MkDirResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_MK_DIR, getCallOptions()), request, responseObserver);
    }

    @Override
    public void shutdown(com.quick.dfs.namenode.rpc.model.ShutdownRequest request,
        io.grpc.stub.StreamObserver<com.quick.dfs.namenode.rpc.model.ShutdownResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_SHUTDOWN, getCallOptions()), request, responseObserver);
    }

    @Override
    public void fetchEditLog(com.quick.dfs.namenode.rpc.model.FetchEditLogRequest request,
        io.grpc.stub.StreamObserver<com.quick.dfs.namenode.rpc.model.FetchEditLogResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_FETCH_EDIT_LOG, getCallOptions()), request, responseObserver);
    }

    @Override
    public void updateCheckpointTxid(com.quick.dfs.namenode.rpc.model.UpdateCheckpointTxidRequest request,
        io.grpc.stub.StreamObserver<com.quick.dfs.namenode.rpc.model.UpdateCheckpointTxidResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_UPDATE_CHECKPOINT_TXID, getCallOptions()), request, responseObserver);
    }

    @Override
    public void createFile(com.quick.dfs.namenode.rpc.model.CreateFileRequest request,
        io.grpc.stub.StreamObserver<com.quick.dfs.namenode.rpc.model.CreateFileResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_CREATE_FILE, getCallOptions()), request, responseObserver);
    }

    @Override
    public void allocateDataNodes(com.quick.dfs.namenode.rpc.model.AllocateDataNodesRequest request,
        io.grpc.stub.StreamObserver<com.quick.dfs.namenode.rpc.model.AllocateDataNodesResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_ALLOCATE_DATA_NODES, getCallOptions()), request, responseObserver);
    }
  }

  public static class NameNodeServiceBlockingStub extends io.grpc.stub.AbstractStub<NameNodeServiceBlockingStub>
      implements NameNodeServiceBlockingClient {
    private NameNodeServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private NameNodeServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected NameNodeServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new NameNodeServiceBlockingStub(channel, callOptions);
    }

    @Override
    public com.quick.dfs.namenode.rpc.model.RegisterResponse register(com.quick.dfs.namenode.rpc.model.RegisterRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_REGISTER, getCallOptions(), request);
    }

    @Override
    public com.quick.dfs.namenode.rpc.model.HeartbeatResponse heartbeat(com.quick.dfs.namenode.rpc.model.HeartbeatRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_HEARTBEAT, getCallOptions(), request);
    }

    @Override
    public com.quick.dfs.namenode.rpc.model.MkDirResponse mkDir(com.quick.dfs.namenode.rpc.model.MkDirRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_MK_DIR, getCallOptions(), request);
    }

    @Override
    public com.quick.dfs.namenode.rpc.model.ShutdownResponse shutdown(com.quick.dfs.namenode.rpc.model.ShutdownRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_SHUTDOWN, getCallOptions(), request);
    }

    @Override
    public com.quick.dfs.namenode.rpc.model.FetchEditLogResponse fetchEditLog(com.quick.dfs.namenode.rpc.model.FetchEditLogRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_FETCH_EDIT_LOG, getCallOptions(), request);
    }

    @Override
    public com.quick.dfs.namenode.rpc.model.UpdateCheckpointTxidResponse updateCheckpointTxid(com.quick.dfs.namenode.rpc.model.UpdateCheckpointTxidRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_UPDATE_CHECKPOINT_TXID, getCallOptions(), request);
    }

    @Override
    public com.quick.dfs.namenode.rpc.model.CreateFileResponse createFile(com.quick.dfs.namenode.rpc.model.CreateFileRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_CREATE_FILE, getCallOptions(), request);
    }

    @Override
    public com.quick.dfs.namenode.rpc.model.AllocateDataNodesResponse allocateDataNodes(com.quick.dfs.namenode.rpc.model.AllocateDataNodesRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_ALLOCATE_DATA_NODES, getCallOptions(), request);
    }
  }

  public static class NameNodeServiceFutureStub extends io.grpc.stub.AbstractStub<NameNodeServiceFutureStub>
      implements NameNodeServiceFutureClient {
    private NameNodeServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private NameNodeServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected NameNodeServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new NameNodeServiceFutureStub(channel, callOptions);
    }

    @Override
    public com.google.common.util.concurrent.ListenableFuture<com.quick.dfs.namenode.rpc.model.RegisterResponse> register(
        com.quick.dfs.namenode.rpc.model.RegisterRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_REGISTER, getCallOptions()), request);
    }

    @Override
    public com.google.common.util.concurrent.ListenableFuture<com.quick.dfs.namenode.rpc.model.HeartbeatResponse> heartbeat(
        com.quick.dfs.namenode.rpc.model.HeartbeatRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_HEARTBEAT, getCallOptions()), request);
    }

    @Override
    public com.google.common.util.concurrent.ListenableFuture<com.quick.dfs.namenode.rpc.model.MkDirResponse> mkDir(
        com.quick.dfs.namenode.rpc.model.MkDirRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_MK_DIR, getCallOptions()), request);
    }

    @Override
    public com.google.common.util.concurrent.ListenableFuture<com.quick.dfs.namenode.rpc.model.ShutdownResponse> shutdown(
        com.quick.dfs.namenode.rpc.model.ShutdownRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_SHUTDOWN, getCallOptions()), request);
    }

    @Override
    public com.google.common.util.concurrent.ListenableFuture<com.quick.dfs.namenode.rpc.model.FetchEditLogResponse> fetchEditLog(
        com.quick.dfs.namenode.rpc.model.FetchEditLogRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_FETCH_EDIT_LOG, getCallOptions()), request);
    }

    @Override
    public com.google.common.util.concurrent.ListenableFuture<com.quick.dfs.namenode.rpc.model.UpdateCheckpointTxidResponse> updateCheckpointTxid(
        com.quick.dfs.namenode.rpc.model.UpdateCheckpointTxidRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_UPDATE_CHECKPOINT_TXID, getCallOptions()), request);
    }

    @Override
    public com.google.common.util.concurrent.ListenableFuture<com.quick.dfs.namenode.rpc.model.CreateFileResponse> createFile(
        com.quick.dfs.namenode.rpc.model.CreateFileRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_CREATE_FILE, getCallOptions()), request);
    }

    @Override
    public com.google.common.util.concurrent.ListenableFuture<com.quick.dfs.namenode.rpc.model.AllocateDataNodesResponse> allocateDataNodes(
        com.quick.dfs.namenode.rpc.model.AllocateDataNodesRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_ALLOCATE_DATA_NODES, getCallOptions()), request);
    }
  }

  private static final int METHODID_REGISTER = 0;
  private static final int METHODID_HEARTBEAT = 1;
  private static final int METHODID_MK_DIR = 2;
  private static final int METHODID_SHUTDOWN = 3;
  private static final int METHODID_FETCH_EDIT_LOG = 4;
  private static final int METHODID_UPDATE_CHECKPOINT_TXID = 5;
  private static final int METHODID_CREATE_FILE = 6;
  private static final int METHODID_ALLOCATE_DATA_NODES = 7;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final NameNodeService serviceImpl;
    private final int methodId;

    public MethodHandlers(NameNodeService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_REGISTER:
          serviceImpl.register((com.quick.dfs.namenode.rpc.model.RegisterRequest) request,
              (io.grpc.stub.StreamObserver<com.quick.dfs.namenode.rpc.model.RegisterResponse>) responseObserver);
          break;
        case METHODID_HEARTBEAT:
          serviceImpl.heartbeat((com.quick.dfs.namenode.rpc.model.HeartbeatRequest) request,
              (io.grpc.stub.StreamObserver<com.quick.dfs.namenode.rpc.model.HeartbeatResponse>) responseObserver);
          break;
        case METHODID_MK_DIR:
          serviceImpl.mkDir((com.quick.dfs.namenode.rpc.model.MkDirRequest) request,
              (io.grpc.stub.StreamObserver<com.quick.dfs.namenode.rpc.model.MkDirResponse>) responseObserver);
          break;
        case METHODID_SHUTDOWN:
          serviceImpl.shutdown((com.quick.dfs.namenode.rpc.model.ShutdownRequest) request,
              (io.grpc.stub.StreamObserver<com.quick.dfs.namenode.rpc.model.ShutdownResponse>) responseObserver);
          break;
        case METHODID_FETCH_EDIT_LOG:
          serviceImpl.fetchEditLog((com.quick.dfs.namenode.rpc.model.FetchEditLogRequest) request,
              (io.grpc.stub.StreamObserver<com.quick.dfs.namenode.rpc.model.FetchEditLogResponse>) responseObserver);
          break;
        case METHODID_UPDATE_CHECKPOINT_TXID:
          serviceImpl.updateCheckpointTxid((com.quick.dfs.namenode.rpc.model.UpdateCheckpointTxidRequest) request,
              (io.grpc.stub.StreamObserver<com.quick.dfs.namenode.rpc.model.UpdateCheckpointTxidResponse>) responseObserver);
          break;
        case METHODID_CREATE_FILE:
          serviceImpl.createFile((com.quick.dfs.namenode.rpc.model.CreateFileRequest) request,
              (io.grpc.stub.StreamObserver<com.quick.dfs.namenode.rpc.model.CreateFileResponse>) responseObserver);
          break;
        case METHODID_ALLOCATE_DATA_NODES:
          serviceImpl.allocateDataNodes((com.quick.dfs.namenode.rpc.model.AllocateDataNodesRequest) request,
              (io.grpc.stub.StreamObserver<com.quick.dfs.namenode.rpc.model.AllocateDataNodesResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static io.grpc.ServerServiceDefinition bindService(
      final NameNodeService serviceImpl) {
    return io.grpc.ServerServiceDefinition.builder(SERVICE_NAME)
        .addMethod(
          METHOD_REGISTER,
          asyncUnaryCall(
            new MethodHandlers<
              com.quick.dfs.namenode.rpc.model.RegisterRequest,
              com.quick.dfs.namenode.rpc.model.RegisterResponse>(
                serviceImpl, METHODID_REGISTER)))
        .addMethod(
          METHOD_HEARTBEAT,
          asyncUnaryCall(
            new MethodHandlers<
              com.quick.dfs.namenode.rpc.model.HeartbeatRequest,
              com.quick.dfs.namenode.rpc.model.HeartbeatResponse>(
                serviceImpl, METHODID_HEARTBEAT)))
        .addMethod(
          METHOD_MK_DIR,
          asyncUnaryCall(
            new MethodHandlers<
              com.quick.dfs.namenode.rpc.model.MkDirRequest,
              com.quick.dfs.namenode.rpc.model.MkDirResponse>(
                serviceImpl, METHODID_MK_DIR)))
        .addMethod(
          METHOD_SHUTDOWN,
          asyncUnaryCall(
            new MethodHandlers<
              com.quick.dfs.namenode.rpc.model.ShutdownRequest,
              com.quick.dfs.namenode.rpc.model.ShutdownResponse>(
                serviceImpl, METHODID_SHUTDOWN)))
        .addMethod(
          METHOD_FETCH_EDIT_LOG,
          asyncUnaryCall(
            new MethodHandlers<
              com.quick.dfs.namenode.rpc.model.FetchEditLogRequest,
              com.quick.dfs.namenode.rpc.model.FetchEditLogResponse>(
                serviceImpl, METHODID_FETCH_EDIT_LOG)))
        .addMethod(
          METHOD_UPDATE_CHECKPOINT_TXID,
          asyncUnaryCall(
            new MethodHandlers<
              com.quick.dfs.namenode.rpc.model.UpdateCheckpointTxidRequest,
              com.quick.dfs.namenode.rpc.model.UpdateCheckpointTxidResponse>(
                serviceImpl, METHODID_UPDATE_CHECKPOINT_TXID)))
        .addMethod(
          METHOD_CREATE_FILE,
          asyncUnaryCall(
            new MethodHandlers<
              com.quick.dfs.namenode.rpc.model.CreateFileRequest,
              com.quick.dfs.namenode.rpc.model.CreateFileResponse>(
                serviceImpl, METHODID_CREATE_FILE)))
        .addMethod(
          METHOD_ALLOCATE_DATA_NODES,
          asyncUnaryCall(
            new MethodHandlers<
              com.quick.dfs.namenode.rpc.model.AllocateDataNodesRequest,
              com.quick.dfs.namenode.rpc.model.AllocateDataNodesResponse>(
                serviceImpl, METHODID_ALLOCATE_DATA_NODES)))
        .build();
  }
}
