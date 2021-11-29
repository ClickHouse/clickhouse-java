package ru.yandex.clickhouse.util;

import org.testng.Assert;
import org.testng.annotations.Test;
import ru.yandex.clickhouse.response.ClickHouseLZ4Stream;

/**
 * @author Anton Sukhonosenko <a href="mailto:algebraic@yandex-team.ru"></a>
 * @date 08.06.18
 */
public class ClickHouseBlockChecksumTest {
    private static final int HEADER_SIZE_BYTES = 9;

    private static int hexToBin(char ch) {
        if ('0' <= ch && ch <= '9') {
            return ch - '0';
        }
        if ('A' <= ch && ch <= 'F') {
            return ch - 'A' + 10;
        }
        if ('a' <= ch && ch <= 'f') {
            return ch - 'a' + 10;
        }
        return -1;
    }

    private static byte[] parseHexBinary(String s) {
        final int len = s.length();

        // "111" is not a valid hex encoding.
        if (len % 2 != 0) {
            throw new IllegalArgumentException("hexBinary needs to be even-length: " + s);
        }

        byte[] out = new byte[len / 2];

        for (int i = 0; i < len; i += 2) {
            int h = hexToBin(s.charAt(i));
            int l = hexToBin(s.charAt(i + 1));
            if (h == -1 || l == -1) {
                throw new IllegalArgumentException("contains illegal character for hexBinary: " + s);
            }

            out[i / 2] = (byte) (h * 16 + l);
        }

        return out;
    }

    @Test(groups = "unit")
    public void trickyBlock() {
        byte[] compressedData = parseHexBinary("1F000100078078000000B4000000");
        int uncompressedSizeBytes = 35;

        ClickHouseBlockChecksum checksum = ClickHouseBlockChecksum.calculateForBlock(
                (byte) ClickHouseLZ4Stream.MAGIC,
                compressedData.length + HEADER_SIZE_BYTES,
                uncompressedSizeBytes,
                compressedData,
                compressedData.length);

        Assert.assertEquals(
                new ClickHouseBlockChecksum(-493639813825217902L, -6253550521065361778L),
                checksum);
    }

    @Test(groups = "unit")
    public void anotherTrickyBlock() {
        byte[] compressedData = parseHexBinary("80D9CEF753E3A59B3F");
        int uncompressedSizeBytes = 8;

        ClickHouseBlockChecksum checksum = ClickHouseBlockChecksum.calculateForBlock(
                (byte) ClickHouseLZ4Stream.MAGIC,
                compressedData.length + HEADER_SIZE_BYTES,
                uncompressedSizeBytes,
                compressedData,
                compressedData.length);

        Assert.assertEquals(
                new ClickHouseBlockChecksum(-7135037831041210418L, -8214889029657590490L),
                checksum);
    }
}