package cn.edu.nju.pasalab.graph.kvstore;

import cn.edu.nju.pasalab.graph.storage.Serializer;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class KVStoreClient {
    final String hosts[];
    final int ports[];
    final ManagedChannel channels[];
    final KVStoreServerGrpc.KVStoreServerStub stubs[];
    final Empty empty = Empty.newBuilder().build();

    public static int hashedHostID(long key, int numHosts) {
        return (int)(Math.abs(key) % numHosts);
    }

    public static int hashedHostID(byte key[], int numHosts) {
        return (int)(Math.abs(java.util.Arrays.hashCode(key)) % numHosts);
    }

    public KVStoreClient(String hosts[], int ports[]) {
        assert hosts.length == ports.length;
        this.hosts = hosts.clone();
        this.ports = ports.clone();
        channels = new ManagedChannel[hosts.length];
        stubs = new KVStoreServerGrpc.KVStoreServerStub[hosts.length];
        for (int i = 0; i < hosts.length; i++) {
            channels[i] = ManagedChannelBuilder.forAddress(hosts[i], ports[i]).usePlaintext(true).build();
            stubs[i] = KVStoreServerGrpc.newStub(channels[i]);
        }
    }

    public void clear() throws InterruptedException {
       for (int i = 0; i < hosts.length; i++) {
           KVStoreServerGrpc.KVStoreServerBlockingStub stub = KVStoreServerGrpc.newBlockingStub(channels[i]);
           stub.clear(empty);
        }
    }

    public ListenableFuture<Reply> get(long key) {
        int hostID = hashedHostID(key, hosts.length);
        KVStoreServerGrpc.KVStoreServerFutureStub stub = KVStoreServerGrpc.newFutureStub(channels[hostID]);
        Query query = Query.newBuilder().setKey(key).setRequestor(1L).build();
        return  stub.get(query);
    }

    public byte[] get(byte key[]) {
        int hostID = hashedHostID(key, hosts.length);
        KVStoreServerGrpc.KVStoreServerBlockingStub stub = KVStoreServerGrpc.newBlockingStub(channels[hostID]);
        BinQuery query = BinQuery.newBuilder().setKey(ByteString.copyFrom(key)).setRequestor(1L).build();
        BinReply reply = stub.binGet(query);
        if (reply == null) return null;
        return reply.getValue().toByteArray();
    }

    public StreamingPutter streamingPut() {
        return new StreamingPutter();
    }

    public StreamingGetter streamingGet(int maxBatchSize) {
        return new StreamingGetter();
    }

    public StreamingBinPutter streamingBinPut() {
        return new StreamingBinPutter();
    }

    public StreamingBinGetter streamingBinGetter() {
        return new StreamingBinGetter();
    }

    public class StreamingPutter {

        private final StreamObserver<Put> putObservers[];
        private final CountDownLatch finishLatches[];

        public StreamingPutter() {
            putObservers = new StreamObserver[hosts.length];
            finishLatches = new CountDownLatch[hosts.length];
            for (int i = 0; i < hosts.length; i++) {
                final int index = i;
                finishLatches[index] = new CountDownLatch(1);
                putObservers[index] = stubs[index].streamPut(new StreamObserver<Empty>() {
                    @Override
                    public void onNext(Empty empty) {}

                    @Override
                    public void onError(Throwable throwable) {
                        finishLatches[index].countDown();
                        System.err.println(throwable);
                        System.exit(2);
                    }

                    @Override
                    public void onCompleted() {
                        finishLatches[index].countDown();
                    }
                });
            }
        }

        public void put(long key, long adj[]) throws IOException {
            ByteString value = ByteString.copyFrom(Serializer.serialize(adj));
            Put put = Put.newBuilder().setKey(key).setValue(value).build();
            int hostID = hashedHostID(key, hosts.length);
            putObservers[hostID].onNext(put);
        }

        public void finish() throws InterruptedException {
            for (int i = 0; i < putObservers.length; i++) {
                putObservers[i].onCompleted();
            }
            for (int i = 0; i < putObservers.length; i++) {
                finishLatches[i].await();
            }
        }
    }

    public class StreamingBinPutter {

        private final StreamObserver<BinPut> putObservers[];
        private final CountDownLatch finishLatches[];

        public StreamingBinPutter() {
            putObservers = new StreamObserver[hosts.length];
            finishLatches = new CountDownLatch[hosts.length];
            for (int i = 0; i < hosts.length; i++) {
                final int index = i;
                finishLatches[index] = new CountDownLatch(1);
                putObservers[index] = stubs[index].streamBinPut(new StreamObserver<Empty>() {
                    @Override
                    public void onNext(Empty empty) {}

                    @Override
                    public void onError(Throwable throwable) {
                        finishLatches[index].countDown();
                        System.err.println(throwable);
                        System.exit(2);
                    }

                    @Override
                    public void onCompleted() {
                        finishLatches[index].countDown();
                    }
                });
            }
        }

        public void put(byte key[], byte value[]) throws IOException {
            BinPut put = BinPut.newBuilder().setKey(ByteString.copyFrom(key))
                    .setValue(ByteString.copyFrom(value)).build();
            int hostID = hashedHostID(key, hosts.length);
            putObservers[hostID].onNext(put);
        }

        public void finish() throws InterruptedException {
            for (int i = 0; i < putObservers.length; i++) {
                putObservers[i].onCompleted();
            }
            for (int i = 0; i < putObservers.length; i++) {
                finishLatches[i].await();
            }
        }
    }


    public class StreamingGetter {
        private final StreamObserver<Query> queryObservers[];
        private final CountDownLatch finishLatches[];
        private int conductedQueries = 0;
        private int retrievedQueries = 0;
        private final BlockingQueue<Reply> replies;
        private Throwable exception;

        public StreamingGetter() {
            queryObservers = new StreamObserver[hosts.length];
            finishLatches = new CountDownLatch[hosts.length];
            replies = new LinkedBlockingQueue<>();
            for (int i = 0; i < hosts.length; i++) {
                final int index = i;
                finishLatches[index] = new CountDownLatch(1);
                queryObservers[index] = stubs[index].streamGet(new StreamObserver<Reply>() {
                    @Override
                    public void onNext(Reply reply) {
                        try {
                            replies.put(reply);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                            exception = e;
                        }
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        finishLatches[index].countDown();
                        System.err.println(throwable);
                        exception = throwable;
                    }

                    @Override
                    public void onCompleted() {
                        finishLatches[index].countDown();
                    }
                });
            }
        }

        public void get(long key, long requestor) throws Throwable {
            if (exception != null) throw exception;
            Query query = Query.newBuilder().setKey(key).setRequestor(requestor).build();
            int hostID = hashedHostID(key, hosts.length);
            conductedQueries += 1;
            queryObservers[hostID].onNext(query);
        }

        public Reply nextReply() throws Throwable {
            if (exception != null) throw (exception);
            Reply r = null;
            long t = 0;
            while (exception  == null && r == null && t < 3) {
                r = replies.poll(10, TimeUnit.SECONDS);
                t += 1;
            }
            if (r == null  && t >= 3) throw new Exception("KVStoreClient streamGetter nextReply Timeout");
            if (exception != null) throw (exception);
            return r;
        }

        public void finish() throws InterruptedException {
            for (int i = 0; i < queryObservers.length; i++) {
                queryObservers[i].onCompleted();
            }
        }

        public void waitForComplete() throws InterruptedException {
            for (int i = 0; i < hosts.length; i++) {
                finishLatches[i].await();
            }
        }

    }

    public class StreamingBinGetter {
        private final StreamObserver<BinQuery> queryObservers[];
        private final CountDownLatch finishLatches[];
        private int conductedQueries = 0;
        private int retrievedQueries = 0;
        private final BlockingQueue<BinReply> replies;
        private Throwable exception;

        public StreamingBinGetter() {
            queryObservers = new StreamObserver[hosts.length];
            finishLatches = new CountDownLatch[hosts.length];
            replies = new LinkedBlockingQueue<>();
            for (int i = 0; i < hosts.length; i++) {
                final int index = i;
                finishLatches[index] = new CountDownLatch(1);
                queryObservers[index] = stubs[index].streamBinGet(new StreamObserver<BinReply>() {
                    @Override
                    public void onNext(BinReply reply) {
                        try {
                            replies.put(reply);
                        } catch (InterruptedException e) {
                            finishLatches[index].countDown();
                            e.printStackTrace();
                            exception = e;
                        }
                    }
                    @Override
                    public void onError(Throwable throwable) {
                        finishLatches[index].countDown();
                        System.err.println(throwable);
                        exception = throwable;
                    }
                    @Override
                    public void onCompleted() {
                        finishLatches[index].countDown();
                    }
                });
            }
        }

        public void get(byte key[], long requestor) throws Throwable {
            if (exception != null) throw exception;
            BinQuery query = BinQuery.newBuilder().setKey(ByteString.copyFrom(key)).setRequestor(requestor).build();
            int hostID = hashedHostID(key, hosts.length);
            conductedQueries += 1;
            queryObservers[hostID].onNext(query);
        }

        public BinReply nextReply() throws Throwable {
            if (exception != null) throw (exception);
            BinReply r = null;
            long t = 0;
            while (exception  == null && r == null && t < 3) {
                r = replies.poll(10, TimeUnit.SECONDS);
                t += 1;
            }
            if (r == null  && t >= 3) {
                int i = 0;
                for (i = 0; i < finishLatches.length; i++) {
                    if (finishLatches[i].getCount() > 0) break;
                }
                if (i == finishLatches.length) throw new Exception("KVStoreClient: streamBinGetter: all connections are closed, but now we want to wait for it");
                throw new Exception("KVStoreClient streamBinGetter nextReply Timeout");
            }
            if (exception != null) throw (exception);
            return r;
        }

        public void finish() throws InterruptedException {
            for (int i = 0; i < queryObservers.length; i++) {
                queryObservers[i].onCompleted();
            }
        }

        public void waitForComplete() throws InterruptedException {
            for (int i = 0; i < hosts.length; i++) {
                finishLatches[i].await();
            }
        }
    }

    public void close() {
        if (stubs != null) {
            for (int i = 0; i < stubs.length; i++) {
                stubs[i] = null;
            }
        }
        if (channels != null) {
            for (int i = 0; i < channels.length; i++) {
                if (channels[i] != null)
                    channels[i].shutdown();
            }
        }
    }
}
