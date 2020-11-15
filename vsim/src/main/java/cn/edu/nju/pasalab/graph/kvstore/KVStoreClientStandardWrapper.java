package cn.edu.nju.pasalab.graph.kvstore;

import cn.edu.nju.pasalab.db.BasicKVDatabaseClient;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

public class KVStoreClientStandardWrapper extends BasicKVDatabaseClient {
    private KVStoreClient client;

    public KVStoreClientStandardWrapper() {

    }

    @Override
    public byte[] get(byte[] key) throws Exception {
        return client.get(key);
    }

    @Override
    public void put(byte[] key, byte[] value) throws Exception {
        KVStoreClient.StreamingBinPutter putter =  client.streamingBinPut();
        putter.put(key, value);
        putter.finish();
    }

    @Override
    public void putAll(byte[][] keys, byte[][] values) throws Exception {
        if (keys.length != values.length) throw new Exception("Lenghts of keys and values do not match");
        KVStoreClient.StreamingBinPutter putter =  client.streamingBinPut();
        for (int i = 0; i < keys.length; i++) {
            putter.put(keys[i], values[i]);
        }
        putter.finish();
    }

    @Override
    public byte[][] getAll(byte[][] keys) throws Exception {
        try {
            KVStoreClient.StreamingBinGetter getter = client.streamingBinGetter();
            Map<ByteBuffer,byte[]> results = new HashMap<>();
            for (int i = 0; i < keys.length; i++) {
                getter.get(keys[i], 1L);
            }
            getter.finish();
            for (int i = 0; i < keys.length; i++) {
                BinReply reply = getter.nextReply();
                results.put(reply.getKey().asReadOnlyByteBuffer(), reply.getValue().toByteArray());
            }
            byte returned[][] = new byte[keys.length][];
            for (int i = 0; i < keys.length; i++) {
                ByteBuffer key = ByteBuffer.wrap(keys[i]);
                if (!results.containsKey(key)) throw new Exception("key should be contained in the results hash map");
                byte value[] = results.get(key);
                returned[i] = value;
            }
            return returned;
        } catch (Throwable e) {
            throw new Exception(e);
        }
    }

    @Override
    public void close() throws Exception {
        client.close();
        client = null;
    }

    @Override
    public void connect(Properties properties) throws Exception {
        String hostList = properties.getProperty("mykvstore.hostlist");
        String hosts[] = hostList.split(",");
        String hostnames[] = new String[hosts.length];
        int ports[] = new int[hosts.length];
        for (int i = 0; i < hosts.length; i++) {
            hostnames[i] = hosts[i].split(":")[0];
            ports[i] = Integer.parseInt(hosts[i].split(":")[1]);
        }
        if (client != null)
            client.close();
        client = new KVStoreClient(hostnames, ports);
        Logger.getLogger(KVStoreClientStandardWrapper.class.getName()).info("Client created");
    }

    @Override
    public void clearDB() throws Exception {
        client.clear();
    }

    @Override
    public void createDB() throws Exception {

    }
}
