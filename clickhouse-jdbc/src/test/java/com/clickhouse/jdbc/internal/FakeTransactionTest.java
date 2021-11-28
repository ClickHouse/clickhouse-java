package com.clickhouse.jdbc.internal;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;

import com.clickhouse.jdbc.internal.FakeTransaction.FakeSavepoint;

import org.testng.Assert;
import org.testng.annotations.Test;

public class FakeTransactionTest {
    @Test(groups = "unit")
    public void testQuery() {
        FakeTransaction tx = new FakeTransaction();
        Assert.assertNotNull(tx.id);
        Assert.assertEquals(tx.getQueries(), Collections.emptyList());
        Assert.assertEquals(tx.getSavepoints(), Collections.emptyList());

        String queryId = tx.newQuery(null);
        Assert.assertNotNull(queryId);
        Assert.assertEquals(tx.getQueries(), Collections.singleton(queryId));
        Assert.assertEquals(tx.getSavepoints(), Collections.emptyList());

        String newQueryId = tx.newQuery(queryId);
        Assert.assertNotNull(newQueryId);
        Assert.assertNotEquals(newQueryId, queryId);
        Assert.assertEquals(tx.getQueries(), Arrays.asList(queryId, newQueryId));
        Assert.assertEquals(tx.getSavepoints(), Collections.emptyList());

        tx.clear();
        Assert.assertEquals(tx.getQueries(), Collections.emptyList());
        Assert.assertEquals(tx.getSavepoints(), Collections.emptyList());

        queryId = tx.newQuery("");
        Assert.assertNotNull(queryId);
        Assert.assertEquals(tx.getQueries(), Collections.singleton(queryId));
        Assert.assertEquals(tx.getSavepoints(), Collections.emptyList());
    }

    @Test(groups = "unit")
    public void testSavepoint() throws SQLException {
        FakeTransaction tx = new FakeTransaction();
        Assert.assertNotNull(tx.id);
        Assert.assertEquals(tx.getQueries(), Collections.emptyList());
        Assert.assertEquals(tx.getSavepoints(), Collections.emptyList());

        FakeSavepoint unnamedSavepoint = tx.newSavepoint(null);
        FakeSavepoint s1 = unnamedSavepoint;
        Assert.assertEquals(unnamedSavepoint.id, 0);
        Assert.assertEquals(unnamedSavepoint.getSavepointId(), 0);
        Assert.assertNull(unnamedSavepoint.name, "Un-named savepoint should not have name");
        Assert.assertThrows(SQLException.class, () -> s1.getSavepointName());
        Assert.assertEquals(tx.getQueries(), Collections.emptyList());
        Assert.assertEquals(tx.getSavepoints(), Collections.singleton(unnamedSavepoint));

        FakeSavepoint namedSavepoint = tx.newSavepoint("tmp");
        FakeSavepoint s2 = namedSavepoint;
        Assert.assertEquals(namedSavepoint.id, 0);
        Assert.assertThrows(SQLException.class, () -> s2.getSavepointId());
        Assert.assertEquals(namedSavepoint.name, "tmp");
        Assert.assertEquals(namedSavepoint.getSavepointName(), "tmp");
        Assert.assertEquals(tx.getQueries(), Collections.emptyList());
        Assert.assertEquals(tx.getSavepoints(), Arrays.asList(unnamedSavepoint, namedSavepoint));

        tx.toSavepoint(namedSavepoint);
        Assert.assertEquals(tx.getQueries(), Collections.emptyList());
        Assert.assertEquals(tx.getSavepoints(), Collections.singleton(unnamedSavepoint));

        tx.toSavepoint(unnamedSavepoint);
        Assert.assertEquals(tx.getQueries(), Collections.emptyList());
        Assert.assertEquals(tx.getSavepoints(), Collections.emptyList());

        tx.clear();
        Assert.assertEquals(tx.getQueries(), Collections.emptyList());
        Assert.assertEquals(tx.getSavepoints(), Collections.emptyList());

        String queryId = tx.newQuery(null);
        FakeSavepoint s3 = unnamedSavepoint = tx.newSavepoint(null);
        Assert.assertEquals(unnamedSavepoint.id, 1);
        Assert.assertEquals(unnamedSavepoint.getSavepointId(), 1);
        Assert.assertNull(unnamedSavepoint.name, "Un-named savepoint should not have name");
        Assert.assertThrows(SQLException.class, () -> s3.getSavepointName());
        Assert.assertEquals(tx.getQueries().size(), 1);
        Assert.assertEquals(tx.getSavepoints(), Collections.singleton(unnamedSavepoint));

        tx.newQuery(null);
        FakeSavepoint s4 = namedSavepoint = tx.newSavepoint("tmp");
        Assert.assertEquals(namedSavepoint.id, 2);
        Assert.assertThrows(SQLException.class, () -> s4.getSavepointId());
        Assert.assertEquals(namedSavepoint.name, "tmp");
        Assert.assertEquals(namedSavepoint.getSavepointName(), "tmp");
        Assert.assertEquals(tx.getQueries().size(), 2);
        Assert.assertEquals(tx.getSavepoints(), Arrays.asList(unnamedSavepoint, namedSavepoint));

        tx.toSavepoint(unnamedSavepoint);
        Assert.assertEquals(tx.getQueries(), Collections.singleton(queryId));
        Assert.assertEquals(tx.getSavepoints(), Collections.emptyList());
    }
}
