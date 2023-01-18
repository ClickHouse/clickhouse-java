package com.clickhouse.data.value;

import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.clickhouse.data.ClickHouseDataType;

public class ClickHouseGeoPointValueTest {
    @Test(groups = { "unit" })
    public void testUpdate() {
        ClickHouseGeoPointValue v = ClickHouseGeoPointValue.of(new double[] { 1D, 2D });
        Assert.assertFalse(v.isNullOrEmpty());
        Assert.assertEquals(v.update(new byte[] { (byte) 1, (byte) -2 }).getValue(), new double[] { 1D, -2D });
        Assert.assertEquals(v.update(new Byte[] { (byte) 1, (byte) -2 }).getValue(), new double[] { 1D, -2D });
        Assert.assertEquals(v.update(new short[] { (short) 1, (short) -2 }).getValue(), new double[] { 1D, -2D });
        Assert.assertEquals(v.update(new Short[] { (short) 1, (short) -2 }).getValue(), new double[] { 1D, -2D });
        Assert.assertEquals(v.update(new int[] { 1, -2 }).getValue(), new double[] { 1D, -2D });
        Assert.assertEquals(v.update(new Integer[] { 1, -2 }).getValue(), new double[] { 1D, -2D });
        Assert.assertEquals(v.update(new long[] { 1L, -2L }).getValue(), new double[] { 1D, -2D });
        Assert.assertEquals(v.update(new Long[] { 1L, -2L }).getValue(), new double[] { 1D, -2D });
        Assert.assertEquals(v.update(new float[] { 1F, -2F }).getValue(), new double[] { 1D, -2D });
        Assert.assertEquals(v.update(new Float[] { 1F, -2F }).getValue(), new double[] { 1D, -2D });
        Assert.assertEquals(v.update(new double[] { 233.3D, -233.33D }).getValue(), new double[] { 233.3D, -233.33D });
        Assert.assertEquals(v.update(new Double[] { 233.3D, -233.33D }).getValue(), new double[] { 233.3D, -233.33D });
        Assert.assertEquals(v.update(new Object[] { 233.3D, -233.33D }).getValue(), new double[] { 233.3D, -233.33D });
        Assert.assertEquals(v.update(Arrays.asList(233.3D, -233.33D)).getValue(), new double[] { 233.3D, -233.33D });
        Assert.assertThrows(UnsupportedOperationException.class, () -> v.update(Object.class));
        Assert.assertThrows(UnsupportedOperationException.class, () -> v.update(Enum.class));
        Assert.assertThrows(UnsupportedOperationException.class, () -> v.update(3));
        Assert.assertThrows(UnsupportedOperationException.class, () -> v.update("1"));
        Assert.assertThrows(UnsupportedOperationException.class, () -> v.update(ClickHouseDataType.Array));
    }
}
