package cn.edu.nju.pasalab.graph.partitioning;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.Arrays;
import java.util.Random;

public class Hash {
    /**
     * The interger hash function got from http://www.burtleburtle.net/bob/hash/integer.html.
     * @param a
     * @return
     */
    public static int hashInt(int a) {
        a = (a ^ 61) ^ (a >> 16);
        a = a + (a << 3);
        a = a ^ (a >> 4);
        a = a * 0x27d4eb2d;
        a = a ^ (a >> 15);
        return a;
    }

    public static long hashIntToLong(int a) {
        long x = a;
        x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9L;
        x = (x ^ (x >> 27)) * 0x94d049bb133111ebL;
        x = x ^ (x >> 31);
        return x;
    }
    public static long hash(long x) {
        x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9L;
        x = (x ^ (x >> 27)) * 0x94d049bb133111ebL;
        x = x ^ (x >> 31);
        return x;
    }
    public static long simhash(IntArrayList list) {
        int counts[] = new int[64];
        for (int v: list) {
            long hash = hashIntToLong(v);
            for (int i = 0; i < 64; i++) {
                long flag = hash & 1;
                if (flag > 0) counts[i] += 1; else counts[i] -= 1;
                hash = hash >> 1;
            }
        }
        long hashValue = 0;
        for (int i = 0;i < 64; i++) {
            if (counts[i] > 0) hashValue |= 1 << i;
        }
        return hashValue;
    }

    public static long simhash(long list[]) {
        int counts[] = new int[64];
        for (long v: list) {
            long sig = hash(v);
            for (int i = 0; i < 64; i++) {
                long flag = sig & 1;
                if (flag > 0) counts[i] += 1; else counts[i] -= 1;
                sig = sig >> 1;
            }
        }
        long hashValue = 0;
        for (int i = 0;i < 64; i++) {
            if (counts[i] > 0) hashValue |= 1 << i;
        }
        return hashValue;
    }


    public static int similarBySimHash(long h1, long h2) {
        return 64 - Long.bitCount(h1 ^ h2);
    }

    public static class MinHash {
        private static final long LARGE_PRIME = 2L << 62 - 1;
        private final long[][] hash_coefs;
        private final int n; // number of permutations

        public MinHash(final int numPerm) {
            this.n = numPerm;
            hash_coefs = new long[numPerm][2];
            Random r = new Random();
            r.setSeed(System.nanoTime());
            for (int i = 0; i < numPerm; i++) {
                hash_coefs[i][0] = r.nextLong() % (LARGE_PRIME - 1) + 1; // a
                hash_coefs[i][1] = r.nextLong() % (LARGE_PRIME - 1) + 1; // b
            }
        }

        /**
         * Computes h_i(x) as (a_i * x + b_i) % LARGE_PRIME .
         * @param i
         * @param x
         * @return the hashed value of x, using ith hash function
         */
        private long h(final int i, final long x) {
            return ((hash_coefs[i][0] * x + hash_coefs[i][1]) % LARGE_PRIME);
        }

        public final long[] signature(long set[]) {
            long sig[] = new long[n];
            for (int i = 0; i < n; i++) {
                sig[i] = Long.MAX_VALUE;
            }
            Arrays.sort(set);
            for (final long x: set) {
                for (int i = 0; i < n; i++) {
                    long hashValue = h(i, x);
                    if (hashValue < sig[i]) {
                        sig[i] = hashValue;
                    }
                }
            }
            return sig;
        }

        public final long[] signature(int set[]) {
            long sig[] = new long[n];
            for (int i = 0; i < n; i++) {
                sig[i] = Long.MAX_VALUE;
            }
            Arrays.sort(set);
            for (final int x: set) {
                for (int i = 0; i < n; i++) {
                    long hashValue = h(i, x);
                    if (hashValue < sig[i]) {
                        sig[i] = hashValue;
                    }
                }
            }
            return sig;
        }


        public final double similarity(final long sig1[], final long sig2[]) {
            assert sig1.length == sig2.length;
            if (sig1.length != sig2.length) return Double.NaN;
            int count = 0;
            for (int i = 0; i < n; i++) {
                if (sig1[i] == sig2[i]) count++;
            }
            return count / (double)sig1.length;
        }
    }

    private static final MinHash minhashObj = new MinHash(25);

    public static long[] minhash(long set[]) {
        return minhashObj.signature(set);
    }

    public static long[] minhash(int set[]) {
        return minhashObj.signature(set);
    }


    public static double minhashSimilarity(long sig1[], long sig2[]) {
        return minhashObj.similarity(sig1, sig2);
    }

}


