package cn.edu.nju.pasalab.graph.storage;

import cn.edu.nju.pasalab.db.BasicKVDatabaseClient;
import cn.edu.nju.pasalab.graph.util.ProcessLevelConf;

import java.io.IOException;
import java.util.Properties;

public class SimpleGraphStore {

    public static final String CONF_DB_BACKEND_CLASS_NAME = "graphstore.db.backend.class.name";
    public static final String DEFAULT_DB_BACKEND_CLASS_NAME = "cn.edu.nju.pasalab.db.redis.ShardedRedisClusterClient";


    private final String dbBackendClassName;
    private final BasicKVDatabaseClient db;
    private static SimpleGraphStore singleton = null;
    private static boolean isInited = false;

    public SimpleGraphStore(Properties conf) throws Exception {
        this.dbBackendClassName = conf.getProperty(CONF_DB_BACKEND_CLASS_NAME, DEFAULT_DB_BACKEND_CLASS_NAME);
        db = (BasicKVDatabaseClient) Class.forName(this.dbBackendClassName).newInstance();
        db.connect(conf);
    }

    private static synchronized void initSingleton() throws Exception {
        if (!isInited) {
            Properties conf = ProcessLevelConf.getPasaConf();
            singleton = new SimpleGraphStore(conf);
            isInited = true;
            System.err.println("Process-level simple graph storage singleton created!");
        }
    }

    public static SimpleGraphStore getSingleton() throws Exception {
        if (singleton != null) {
            return singleton;
        } else {
            initSingleton();
        }
        return singleton;
    }

    /**
     * Query the adjacency set of a vertex
     * @param vid
     * @return
     */
    public int[] get(int vid) throws Exception {
        byte[] key = Serializer.serialize(vid);
        byte[] value = db.get(key);
        if (value == null)
            throw new IOException("The vertex key " + vid + " does not exist in the database!.");
        return Serializer.deserializeToIntArray(value);
    }

    public long[] get(long vid) throws Exception {
        byte[] key = Serializer.serialize(vid);
        byte[] value = db.get(key);
        if (value == null)
            throw new IOException("Vertex key " + vid + " does not exist in the DB.");
        return Serializer.deserializeToLongArray(value);
    }

    public void put(long vid, long adj[]) throws Exception {
        byte[] key = Serializer.serialize(vid);
        byte[] value = Serializer.serialize(adj);
        db.put(key, value);
    }

    /**
     * Set the adjacency set of a vertex
     * @param vid
     * @param adj
     */
    public void put(int vid, int adj[]) throws Exception {
        byte[] key = Serializer.serialize(vid);
        byte[] value = Serializer.serialize(adj);
        db.put(key, value);
    }

    public int[][] get(int[] vids) throws Exception {
        byte[][] keys = new byte[vids.length][];
        for(int i = 0; i < vids.length; i++) {
            keys[i] = Serializer.serialize(vids[i]);
        }
        byte[][] values = db.getAll(keys);
        int[][] results = new int[vids.length][];
        for (int i = 0; i < vids.length; i++) {
            results[i] = Serializer.deserializeToIntArray(values[i]);
        }
        return results;
    }

    public long[][] get(long[] vids) throws Exception {
        byte[][] keys = new byte[vids.length][];
        for(int i = 0; i < vids.length; i++) {
            keys[i] = Serializer.serialize(vids[i]);
        }
        byte[][] values = db.getAll(keys);
        long[][] results = new long[vids.length][];
        for (int i = 0; i < vids.length; i++) {
            if (values[i] == null) {
                results[i] = null;
            } else {
                results[i] = Serializer.deserializeToLongArray(values[i]);
            }
        }
        return results;
    }

    public byte[][] getRaw(int[] vids) throws Exception {
        byte[][] keys = new byte[vids.length][];
        for(int i = 0; i < vids.length; i++) {
            keys[i] = Serializer.serialize(vids[i]);
        }
        byte[][] values = db.getAll(keys);
        return values;
    }

    public void put(int[] vids, int[][] adjs) throws Exception {
        byte[][] keys = new byte[vids.length][];
        for(int i = 0; i < vids.length; i++) {
            keys[i] = Serializer.serialize(vids[i]);
        }
        byte[][] values = new byte[vids.length][];
        for (int i = 0; i < vids.length; i++) {
            values[i] = Serializer.serialize(adjs[i]);
        }
        db.putAll(keys, values);
    }

    public void put(long[] vids, long[][] adjs) throws Exception {
        byte[][] keys = new byte[vids.length][];
        for(int i = 0; i < vids.length; i++) {
            keys[i] = Serializer.serialize(vids[i]);
        }
        byte[][] values = new byte[vids.length][];
        for (int i = 0; i < vids.length; i++) {
            values[i] = Serializer.serialize(adjs[i]);
        }
        db.putAll(keys, values);
    }

    public void close() throws Exception {
        db.close();
    }

    public void clearDB() throws Exception {
        db.clearDB();
    }

}
