package com.clickhouse.data.format;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

public class JsonStreamUtilsTest {
    static class JsonCompactResponse {
        private List<Meta> meta;
        private List<List<String>> data;
        private List<String> totals;
        private Extremes extremes;
        private int rows;
        private int rows_before_limit_at_least;

        public static class Extremes {
            private List<String> min;
            private List<String> max;

            public List<String> getMin() {
                return min;
            }

            public void setMin(List<String> min) {
                this.min = min;
            }

            public List<String> getMax() {
                return max;
            }

            public void setMax(List<String> max) {
                this.max = max;
            }
        }

        public static class Meta {
            private String name;
            private String type;

            public String getName() {
                return name;
            }

            public void setName(String name) {
                this.name = name;
            }

            public String getType() {
                return type;
            }

            public void setType(String type) {
                this.type = type;
            }

            @Override
            public String toString() {
                return "Meta{" + "name='" + name + '\'' + ", type='" + type + '\'' + '}';
            }
        }

        public Extremes getExtremes() {
            return extremes;
        }

        public void setExtremes(Extremes extremes) {
            this.extremes = extremes;
        }

        public List<Meta> getMeta() {
            return meta;
        }

        public void setMeta(List<Meta> meta) {
            this.meta = meta;
        }

        public List<List<String>> getData() {
            return data;
        }

        public void setData(List<List<String>> data) {
            this.data = data;
        }

        public int getRows() {
            return rows;
        }

        public void setRows(int rows) {
            this.rows = rows;
        }

        public int getRows_before_limit_at_least() {
            return rows_before_limit_at_least;
        }

        public void setRows_before_limit_at_least(int rows_before_limit_at_least) {
            this.rows_before_limit_at_least = rows_before_limit_at_least;
        }

        public List<String> getTotals() {
            return totals;
        }

        public void setTotals(List<String> totals) {
            this.totals = totals;
        }

        @Override
        public String toString() {
            return "ClickHouseResponse{" + "meta=" + meta + ", data=" + data + ", rows=" + rows
                    + ", rows_before_limit_at_least=" + rows_before_limit_at_least + '}';
        }
    }

    static class JsonResponseSummary {
        private final long read_rows; // number of read rows for selects (may be more than rows in result set)
        private final long written_rows; // number of written rows for inserts
        private final long read_bytes;
        private final long written_bytes;
        private final long total_rows_to_read;

        public JsonResponseSummary(long read_rows, long written_rows, long read_bytes, long written_bytes,
                long total_rows_to_read) {
            this.read_rows = read_rows;
            this.written_rows = written_rows;
            this.read_bytes = read_bytes;
            this.written_bytes = written_bytes;
            this.total_rows_to_read = total_rows_to_read;
        }

        public long getReadRows() {
            return read_rows;
        }

        public long getWrittenRows() {
            return written_rows;
        }

        public long getReadBytes() {
            return read_bytes;
        }

        public long getWrittenBytes() {
            return written_bytes;
        }

        public long getTotalRowsToRead() {
            return total_rows_to_read;
        }
    }

    @Test(groups = { "unit" })
    public void testReadObject() throws IOException {
        Assert.assertThrows(NullPointerException.class,
                () -> JsonStreamUtils.readObject((InputStream) null, JsonCompactResponse.class));
        Assert.assertThrows(NullPointerException.class,
                () -> JsonStreamUtils.readObject((Reader) null, JsonCompactResponse.class));

        Assert.assertNull(JsonStreamUtils.readObject((String) null, JsonCompactResponse.class));
        Assert.assertNull(JsonStreamUtils.readObject("", JsonCompactResponse.class));
        Assert.assertNull(JsonStreamUtils.readObject(new ByteArrayInputStream(new byte[0]), JsonCompactResponse.class));

        JsonCompactResponse r = JsonStreamUtils.readObject("{}", JsonCompactResponse.class);
        Assert.assertNotNull(r);
        Assert.assertNull(r.getMeta());
        Assert.assertNull(r.getData());
        Assert.assertNull(r.getTotals());
        Assert.assertNull(r.getExtremes());
        Assert.assertEquals(r.getRows(), 0);
        Assert.assertEquals(r.getRows_before_limit_at_least(), 0);

        // set extremes=1
        // select 123 as a group by a with totals limit 5 format JSONCompact
        String json = "{\"meta\":[{\"name\":\"123\",\"type\":\"UInt8\"}],\"data\":[[123]],\"totals\":[123],"
                + "\"extremes\":{\"min\":[123],\"max\":[123]},\"rows\":1,\"rows_before_limit_at_least\":1,"
                + "\"statistics\":{\"elapsed\":0.0008974,\"rows_read\":1,\"bytes_read\":1}}";

        r = JsonStreamUtils.readObject(new ByteArrayInputStream(json.getBytes(StandardCharsets.US_ASCII)),
                JsonCompactResponse.class);
        Assert.assertNotNull(r);
        Assert.assertEquals(r.getMeta().size(), 1);
        Assert.assertEquals(r.getMeta().get(0).toString(), "Meta{name='123\', type='UInt8\'}");

        Assert.assertEquals(r.getData().size(), 1);
        Assert.assertEquals(r.getData().get(0), Collections.singleton("123"));

        Assert.assertEquals(r.getTotals(), Collections.singleton("123"));

        Assert.assertNotNull(r.getExtremes());
        Assert.assertEquals(r.getExtremes().getMin(), Collections.singleton("123"));
        Assert.assertEquals(r.getExtremes().getMax(), Collections.singleton("123"));

        Assert.assertEquals(r.getRows(), 1);
        Assert.assertEquals(r.getRows_before_limit_at_least(), 1);

        // map based on property name
        JsonResponseSummary rs = JsonStreamUtils.readObject(
                "{\"read_rows\":1,\"written_rows\":2,\"read_bytes\":3,\"written_bytes\":4,\"total_rows_to_read\":5}",
                JsonResponseSummary.class);

        Assert.assertNotNull(rs);
        Assert.assertEquals(rs.getReadRows(), 1);
        Assert.assertEquals(rs.getWrittenRows(), 2);
        Assert.assertEquals(rs.getReadBytes(), 3);
        Assert.assertEquals(rs.getWrittenBytes(), 4);
        Assert.assertEquals(rs.getTotalRowsToRead(), 5);
    }

    @Test(groups = { "unit" })
    public void testToJsonString() {
        Assert.assertEquals(JsonStreamUtils.toJsonString(null), "null");
        Assert.assertEquals(JsonStreamUtils.toJsonString(""), "\"\"");
        Assert.assertEquals(JsonStreamUtils.toJsonString(1), "1");
        Assert.assertEquals(JsonStreamUtils.toJsonString(new Object()), "{}");
        Assert.assertEquals(JsonStreamUtils.toJsonString(new JsonCompactResponse()),
                "{\"rows\":0,\"rows_before_limit_at_least\":0}");
    }

    @Test(groups = { "unit" })
    public void testWriteObject() throws IOException {
        try (ByteArrayOutputStream output = new ByteArrayOutputStream(1024)) {
            JsonStreamUtils.writeObject(output, new JsonCompactResponse());
            output.flush();
            Assert.assertEquals(new String(output.toByteArray()), "{\"rows\":0,\"rows_before_limit_at_least\":0}");
        }
    }
}
