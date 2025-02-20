/*
 * Copyright 2023 ClickHouse, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2012 tamtam180
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.clickhouse.data;

/**
 * @author tamtam180 - kirscheless at gmail.com
 * @see http://google-opensource.blogspot.jp/2011/04/introducing-cityhash.html
 * @see http://code.google.com/p/cityhash/
 *
 */

/**
 * NOTE: The code is modified to be compatible with CityHash128 used in
 * ClickHouse
 */
@Deprecated
public class ClickHouseCityHash {

    private static final long k0 = 0xc3a5c85c97cb3127L;
    private static final long k1 = 0xb492b66fbe98f273L;
    private static final long k2 = 0x9ae16a3b2f90404fL;
    private static final long k3 = 0xc949d7c7509e6557L;

    private static long toLongLE(byte[] b, int i) {
        return 0xffffffffffffffffL & (((long) b[i + 7] << 56) + ((long) (b[i + 6] & 255) << 48)
                + ((long) (b[i + 5] & 255) << 40) + ((long) (b[i + 4] & 255) << 32) + ((long) (b[i + 3] & 255) << 24)
                + ((b[i + 2] & 255) << 16) + ((b[i + 1] & 255) << 8) + ((b[i + 0] & 255)));
    }

    private static long toIntLE(byte[] b, int i) {
        return 0xffffffffL & (((b[i + 3] & 255) << 24) + ((b[i + 2] & 255) << 16) + ((b[i + 1] & 255) << 8)
                + ((b[i + 0] & 255)));
    }

    private static long fetch64(byte[] s, int pos) {
        return toLongLE(s, pos);
    }

    private static long fetch32(byte[] s, int pos) {
        return toIntLE(s, pos);
    }

    private static int staticCastToInt(byte b) {
        return b & 0xFF;
    }

    private static long rotate(long val, int shift) {
        return shift == 0 ? val : (val >>> shift) | (val << (64 - shift));
    }

    private static long rotateByAtLeast1(long val, int shift) {
        return (val >>> shift) | (val << (64 - shift));
    }

    private static long shiftMix(long val) {
        return val ^ (val >>> 47);
    }

    private static final long kMul = 0x9ddfea08eb382d69L;

    private static long hash128to64(long u, long v) {
        long a = (u ^ v) * kMul;
        a ^= (a >>> 47);
        long b = (v ^ a) * kMul;
        b ^= (b >>> 47);
        b *= kMul;
        return b;
    }

    private static long hashLen16(long u, long v) {
        return hash128to64(u, v);
    }

    private static long hashLen0to16(byte[] s, int pos, int len) {
        if (len > 8) {
            long a = fetch64(s, pos + 0);
            long b = fetch64(s, pos + len - 8);
            return hashLen16(a, rotateByAtLeast1(b + len, len)) ^ b;
        }
        if (len >= 4) {
            long a = fetch32(s, pos + 0);
            return hashLen16((a << 3) + len, fetch32(s, pos + len - 4));
        }
        if (len > 0) {
            byte a = s[pos + 0];
            byte b = s[pos + (len >>> 1)];
            byte c = s[pos + len - 1];
            int y = staticCastToInt(a) + (staticCastToInt(b) << 8);
            int z = len + (staticCastToInt(c) << 2);
            return shiftMix(y * k2 ^ z * k3) * k2;
        }
        return k2;
    }

    private static long[] weakHashLen32WithSeeds(long w, long x, long y, long z, long a, long b) {

        a += w;
        b = rotate(b + a + z, 21);
        long c = a;
        a += x;
        a += y;
        b += rotate(a, 44);
        return new long[] { a + z, b + c };
    }

    private static long[] weakHashLen32WithSeeds(byte[] s, int pos, long a, long b) {
        return weakHashLen32WithSeeds(fetch64(s, pos + 0), fetch64(s, pos + 8), fetch64(s, pos + 16),
                fetch64(s, pos + 24), a, b);
    }

    private static long[] cityMurmur(byte[] s, int pos, int len, long seed0, long seed1) {

        long a = seed0;
        long b = seed1;
        long c = 0;
        long d = 0;

        int l = len - 16;
        if (l <= 0) {
            a = shiftMix(a * k1) * k1;
            c = b * k1 + hashLen0to16(s, pos, len);
            d = shiftMix(a + (len >= 8 ? fetch64(s, pos + 0) : c));
        } else {

            c = hashLen16(fetch64(s, pos + len - 8) + k1, a);
            d = hashLen16(b + len, c + fetch64(s, pos + len - 16));
            a += d;

            do {
                a ^= shiftMix(fetch64(s, pos + 0) * k1) * k1;
                a *= k1;
                b ^= a;
                c ^= shiftMix(fetch64(s, pos + 8) * k1) * k1;
                c *= k1;
                d ^= c;
                pos += 16;
                l -= 16;
            } while (l > 0);
        }

        a = hashLen16(a, c);
        b = hashLen16(d, b);

        return new long[] { a ^ b, hashLen16(b, a) };
    }

    private static long[] cityHash128WithSeed(byte[] s, int pos, int len, long seed0, long seed1) {
        if (len < 128) {
            return cityMurmur(s, pos, len, seed0, seed1);
        }

        long[] v = new long[2], w = new long[2];
        long x = seed0;
        long y = seed1;
        long z = k1 * len;
        v[0] = rotate(y ^ k1, 49) * k1 + fetch64(s, pos);
        v[1] = rotate(v[0], 42) * k1 + fetch64(s, pos + 8);
        w[0] = rotate(y + z, 35) * k1 + x;
        w[1] = rotate(x + fetch64(s, pos + 88), 53) * k1;

        // This is the same inner loop as CityHash64(), manually unrolled.
        do {
            x = rotate(x + y + v[0] + fetch64(s, pos + 16), 37) * k1;
            y = rotate(y + v[1] + fetch64(s, pos + 48), 42) * k1;

            x ^= w[1];
            y ^= v[0];

            z = rotate(z ^ w[0], 33);
            v = weakHashLen32WithSeeds(s, pos, v[1] * k1, x + w[0]);
            w = weakHashLen32WithSeeds(s, pos + 32, z + w[1], y);

            {
                long swap = z;
                z = x;
                x = swap;
            }
            pos += 64;
            x = rotate(x + y + v[0] + fetch64(s, pos + 16), 37) * k1;
            y = rotate(y + v[1] + fetch64(s, pos + 48), 42) * k1;
            x ^= w[1];
            y ^= v[0];
            z = rotate(z ^ w[0], 33);
            v = weakHashLen32WithSeeds(s, pos, v[1] * k1, x + w[0]);
            w = weakHashLen32WithSeeds(s, pos + 32, z + w[1], y);
            {
                long swap = z;
                z = x;
                x = swap;
            }
            pos += 64;
            len -= 128;
        } while (len >= 128);

        y += rotate(w[0], 37) * k0 + z;
        x += rotate(v[0] + z, 49) * k0;

        // If 0 < len < 128, hash up to 4 chunks of 32 bytes each from the end of s.
        for (int tail_done = 0; tail_done < len;) {
            tail_done += 32;
            y = rotate(y - x, 42) * k0 + v[1];
            w[0] += fetch64(s, pos + len - tail_done + 16);
            x = rotate(x, 49) * k0 + w[0];
            w[0] += v[0];
            v = weakHashLen32WithSeeds(s, pos + len - tail_done, v[0], v[1]);
        }

        // At this point our 48 bytes of state should contain more than
        // enough information for a strong 128-bit hash. We use two
        // different 48-byte-to-8-byte hashes to get a 16-byte final result.

        x = hashLen16(x, v[0]);
        y = hashLen16(y, w[0]);

        return new long[] { hashLen16(x + v[1], w[1]) + y, hashLen16(x + w[1], y + v[1]) };
    }

    public static long[] cityHash128(byte[] s, int pos, int len) {
        if (len >= 16) {
            return cityHash128WithSeed(s, pos + 16, len - 16, fetch64(s, pos) ^ k3, fetch64(s, pos + 8));
        } else if (len >= 8) {
            return cityHash128WithSeed(new byte[0], 0, 0, fetch64(s, pos) ^ (len * k0), fetch64(s, pos + len - 8) ^ k1);
        } else {
            return cityHash128WithSeed(s, pos, len, k0, k1);
        }
    }
}
