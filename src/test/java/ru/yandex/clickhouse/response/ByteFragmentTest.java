package ru.yandex.clickhouse.response;

import static org.testng.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class ByteFragmentTest {

    @DataProvider(name = "stringEscape")
    public Object[][] stringEscape() {
        return new Object[][]{
                {"abc'xyz", "abc\\'xyz"},
                {"\rabc\n\0xyz\b\f\txyz\\", "\\rabc\\n\\Nxyz\\b\\f\\txyz\\\\"},
                {"\n汉字'", "\\n汉字\\'"},
                {"юникод,'юникод'\n", "юникод,\\'юникод\\'\\n"}
        };
    }

    @Test(dataProvider = "stringEscape")
    public void testEscape(String str, String escapedStr) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteFragment.escape(str.getBytes("UTF-8"), out);
        assertEquals(out.toString("UTF-8"), escapedStr);
    }

    @Test(dataProvider = "stringEscape")
    public void testUnescape(String str, String escapedStr) throws IOException {
        byte[] bytes = escapedStr.getBytes("UTF-8");
        ByteFragment byteFragment = new ByteFragment(bytes,0, bytes.length);
        assertEquals(new String(byteFragment.unescape()), str);
    }

}
