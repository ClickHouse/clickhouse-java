package com.clickhouse.client.api.data_formats.internal;

import org.junit.Assert;
import org.testng.annotations.Test;

public class BinaryStreamReaderTests {


    @Test
    public void testCachedByteAllocator() {
        BinaryStreamReader.CachingByteBufferAllocator allocator = new BinaryStreamReader.CachingByteBufferAllocator();

        for (int i = 0; i < 6; i++) {
            int size = (int) Math.pow(2, i);
            byte[] firstAllocation = allocator.allocate(size);
            byte[] nextAllocation = allocator.allocate(size);
            Assert.assertSame( "Should be the same buffer for size " + size, firstAllocation, nextAllocation);
        }

        for (int i = 6; i < 16; i++) {
            int size = (int) Math.pow(2, i);
            byte[] firstAllocation = allocator.allocate(size);
            byte[] nextAllocation = allocator.allocate(size);
            Assert.assertNotSame(firstAllocation, nextAllocation);
        }
    }
}
