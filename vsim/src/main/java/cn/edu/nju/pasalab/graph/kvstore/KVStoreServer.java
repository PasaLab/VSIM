package cn.edu.nju.pasalab.graph.kvstore;

import cn.edu.nju.pasalab.graph.storage.Serializer;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

public class KVStoreServer {

    public static void main(String[] args) {
        int port = Integer.parseInt(args[0]);
        long maxSize = Long.parseLong(args[1]);
        long avgSize = Long.parseLong(args[2]);
        CountDownLatch finishLatch = new CountDownLatch(1);
        try {
            KVStoreServer server = new KVStoreServer(maxSize, avgSize, port);
            System.out.println("Server start!");
            finishLatch.await();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }


    long maxSize = 0;
    long avgSize = 0;
    final ChronicleMap<byte[], byte[]> offheapStore;
    int port = 0;
    String hostname;
    ServerRPCService rpcService;
    Server rpcServer;

    public int getPort() {
        return port;
    }

    public String getHostname() {
        return hostname;
    }

    public Server getRpcServer() {
        return rpcServer;
    }

    public KVStoreServer(long maxSize, long avgSize) throws IOException {
        port = 8000 + new Random(System.nanoTime()).nextInt(1000);
        this.maxSize = maxSize; this.avgSize = avgSize;
        offheapStore = ChronicleMapBuilder.of(byte[].class, byte[].class).name("local-kv-store").entries(maxSize)
                .averageKey(new byte[8])
                .averageValue(new byte[(int)avgSize]).create();
        rpcService = new ServerRPCService();
        rpcServer = NettyServerBuilder.forPort(port).addService(rpcService).maxConcurrentCallsPerConnection(4).build();
        rpcServer.start();
        hostname = InetAddress.getLocalHost().getHostName();
    }
    public KVStoreServer(long maxSize, long avgSize, int port) throws IOException {
        this.maxSize = maxSize; this.avgSize = avgSize;
        offheapStore = ChronicleMapBuilder.of(byte[].class, byte[].class).name("local-kv-store").entries(maxSize)
                .averageKeySize(8)
                .averageValueSize(avgSize).create();
        this.port = port;
        rpcService = new ServerRPCService();
        rpcServer = NettyServerBuilder.forPort(port).addService(rpcService).maxConcurrentCallsPerConnection(4).build();
        rpcServer.start();
        hostname = InetAddress.getLocalHost().getHostName();
    }

    private class ServerRPCService extends KVStoreServerGrpc.KVStoreServerImplBase {
        final Empty empty = Empty.newBuilder().build();
        @Override
        public StreamObserver<Put> streamPut(StreamObserver<Empty> responseObserver) {
            StreamObserver<Put> observer = new StreamObserver<Put>() {
                @Override
                public void onNext(Put put) {
                        byte key[] = Serializer.serialize(put.getKey());
                        byte value[] = put.getValue().toByteArray();
                        offheapStore.put(key, value);
                    responseObserver.onNext(empty);
                }

                @Override
                public void onError(Throwable throwable) {
                    System.err.println(throwable);
                    throwable.printStackTrace();
                    Logger.getLogger(ServerRPCService.class.getName()).warning("Error occurs in streamPut:" + throwable.getMessage());
                    responseObserver.onError(throwable);
                }

                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                }
            };
            return observer;
        }

        @Override
        public StreamObserver<Query> streamGet(StreamObserver<Reply> responseObserver) {
            StreamObserver<Query> observer = new StreamObserver<Query>() {
                @Override
                public void onNext(Query query) {
                    byte key[] = Serializer.serialize(query.getKey());
                    if (!offheapStore.containsKey(key)) {
                        Reply reply = Reply.newBuilder().setKey(query.getKey())
                                .setRequestor(query.getRequestor()).setValue(ByteString.EMPTY).build();
                        responseObserver.onNext(reply);
                        return;
                    }
                    byte value[] = offheapStore.get(key);
                    Reply reply = null;
                    reply = Reply.newBuilder().setKey(query.getKey())
                            .setValue(ByteString.copyFrom(value)).build();
                    responseObserver.onNext(reply);
                }

                @Override
                public void onError(Throwable throwable) {
                    System.err.println(throwable);
                    throwable.printStackTrace();
                    Logger.getLogger(ServerRPCService.class.getName()).warning("Error occurs in streamGet:" + throwable.getMessage());
                    responseObserver.onError(throwable);
                }

                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                }
            };
            return observer;
        }

        @Override
        public void clear(Empty request, StreamObserver<Empty> responseObserver) {
            offheapStore.clear();
            responseObserver.onNext(empty);
            responseObserver.onCompleted();
        }

        @Override
        public void get(Query request, StreamObserver<Reply> responseObserver) {
            byte key[] = Serializer.serialize(request.getKey());
            if (offheapStore.containsKey(key)) {
                byte value[] = offheapStore.get(key);
                Reply reply = Reply.newBuilder().setKey(request.getKey()).setRequestor(request.getRequestor())
                            .setValue(ByteString.copyFrom(value)).build();
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
                return;
            }
            Reply reply = Reply.newBuilder().setKey(request.getKey()).setRequestor(request.getRequestor())
                    .setValue(ByteString.EMPTY).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        @Override
        public StreamObserver<BinPut> streamBinPut(StreamObserver<Empty> responseObserver) {
            StreamObserver<BinPut> observer = new StreamObserver<BinPut>() {
                @Override
                public void onNext(BinPut put) {
                    byte key[] = put.getKey().toByteArray();
                    byte value[] = put.getValue().toByteArray();
                    offheapStore.put(key, value);
                    responseObserver.onNext(empty);
                }

                @Override
                public void onError(Throwable throwable) {
                    System.err.println(throwable);
                    throwable.printStackTrace();
                    Logger.getLogger(ServerRPCService.class.getName()).warning("Error occurs in streamBinPut:" + throwable.getMessage());
                    responseObserver.onError(throwable);
                }

                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                }
            };
            return observer;
        }

        @Override
        public StreamObserver<BinQuery> streamBinGet(StreamObserver<BinReply> responseObserver) {
            StreamObserver<BinQuery> observer = new StreamObserver<BinQuery>() {
                @Override
                public void onNext(BinQuery query) {
                    byte key[] = query.getKey().toByteArray();
                    if (!offheapStore.containsKey(key)) {
                        BinReply reply = BinReply.newBuilder().setKey(query.getKey())
                                .setRequestor(query.getRequestor()).setValue(ByteString.EMPTY).build();
                        responseObserver.onNext(reply);
                        return;
                    }
                    byte value[] = offheapStore.get(key);
                    BinReply reply = null;
                    reply = BinReply.newBuilder().setKey(query.getKey())
                            .setValue(ByteString.copyFrom(value)).build();
                    responseObserver.onNext(reply);
                }

                @Override
                public void onError(Throwable throwable) {
                    System.err.println(throwable);
                    throwable.printStackTrace();
                    Logger.getLogger(ServerRPCService.class.getName()).warning("Error occurs in streamBinGet:" + throwable.getMessage());
                    responseObserver.onError(throwable);
                }

                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                }
            };
            return observer;
        }

        @Override
        public void binGet(BinQuery request, StreamObserver<BinReply> responseObserver) {
            byte key[] =request.getKey().toByteArray();
            if (offheapStore.containsKey(key)) {
                byte value[] = offheapStore.get(key);
                BinReply reply = BinReply.newBuilder().setKey(request.getKey()).setRequestor(request.getRequestor())
                        .setValue(ByteString.copyFrom(value)).build();
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
                return;
            }
            BinReply reply = BinReply.newBuilder().setKey(request.getKey()).setRequestor(request.getRequestor())
                    .setValue(ByteString.EMPTY).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }
    }
}
