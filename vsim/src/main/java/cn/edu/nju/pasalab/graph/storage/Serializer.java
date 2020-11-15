package cn.edu.nju.pasalab.graph.storage;

import me.lemire.integercompression.IntCompressor;
import org.xerial.snappy.Snappy;
 
import java.io.IOException;


/**
 * Created by wangzhaokang on 2/6/18.
 */
public final class Serializer {
 
    public static byte[] serialize(long key) {
        byte result[] = new byte[8];
        for (int i = 0; i < 8; i++) {
            result[i] = (byte)key;
            key >>= 8;
        }
        return result;
    }

    public static byte[] serialize(int key) {
        byte result[] = new byte[4];
        for (int i = 0; i < 4; i++) {
            result[i] = (byte)key;
            key >>= 8;
        }
        return result;
    }

    public static void serialize(int key, byte buffer[], int offset) {
        for (int i = 0; i < 4; i++) {
            buffer[offset + i] = (byte)key;
            key >>= 8;
        }
    }

    public static byte[] compressedSerialize(long array[]) throws IOException {
        int[] rawIntArray = new int[array.length * 2];
        for (int i = 0; i < array.length; i++) {
            rawIntArray[2 * i] = (int)array[i];
            rawIntArray[2 * i + 1] =  (int)(array[i] >> 32L);
        }
        IntCompressor compressor = new IntCompressor();
        int[] compressedArray = compressor.compress(rawIntArray);
        return serialize(compressedArray);
    }

 
    public static byte[] serialize(long array[]) throws IOException {
        byte result[] = new byte[8 * array.length];
        for (int j = 0; j < array.length; j++) {
            long key = array[j];
            for (int i = 0; i < 8; i++) {
                result[i + 8 * j] = (byte) key;
                key >>= 8;
            }
        }
        return result;
    }

    public static byte[] serialize(int array[]) throws IOException {
        byte result[] = new byte[4 * array.length];
        for (int j = 0; j < array.length; j++) {
            int key = array[j];
            for (int i = 0; i < 4; i++) {
                result[i + 4 * j] = (byte) key;
                key >>= 8;
            }
        }
        return result;
    }

    public static void serialize(int array[], byte buffer[], int offset) {
        for (int j = 0; j < array.length; j++) {
            int key = array[j];
            for (int i = 0; i < 4; i++) {
                buffer[offset + i + 4 * j] = (byte) key;
                key >>= 8;
            }
        }
    }
 
    public static long deserializeToLong(byte data[]) {
        long result = 0L;
        for (int i = 7; i >= 0; i--) {
            result <<= 8;
            result |= ((long)data[i] & 0x0FFL);
        }
        return result;
    }

    public static int deserializeToInt(byte data[], int offset) {
        int result = 0;
        for (int i = 3; i >= 0; i--) {
            result <<= 8;
            result |= data[i + offset] & 0x0FF;
        }
        return result;
    }
 
    public static long[] deserializeToLongArray(byte data[]) throws IOException {
        long[] result = new long[data.length / 8];
        for (int i = 0; i < data.length; i += 8) {
            long value = 0;
            for (int j = 7; j >= 0; j--) {
                value <<= 8;
                value |= ((long)data[i + j] & 0x0FF);
            }
            result[i / 8] = value;
        }
        return result;
    }

    public static long[] compressedDeserializeToLongArray(byte[] data) throws IOException {
        assert data != null;
        IntCompressor compressor = new IntCompressor();
        int[] compressedIntArray = deserializeToIntArray(data);
        int[] uncompressedIntArray = compressor.uncompress(compressedIntArray);
        long[] result = new long[uncompressedIntArray.length / 2];
        for (int i = 0; i < result.length; i++) {
            result[i] = ((long)uncompressedIntArray[2 * i + 1] << 32L) | uncompressedIntArray[2 * i];
        }
        return result;
    }

    public static int[] deserializeToIntArray(byte data[]) throws IOException {
        int[] result = new int[data.length / 4];
        for (int i = 0; i < data.length; i += 4) {
            int value = 0;
            for (int j = 3; j >= 0; j--) {
                value <<= 8;
                value |= ((int)data[i + j] & 0x0FF);
            }
            result[i / 4] = value;
        }
        return result;
    }
}
