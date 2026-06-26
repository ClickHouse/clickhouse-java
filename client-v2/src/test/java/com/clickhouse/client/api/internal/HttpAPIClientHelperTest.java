package com.clickhouse.client.api.internal;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;

public class HttpAPIClientHelperTest {

    // --- Contrast cases: scalar parameters keep their bare, UNQUOTED text form (unchanged behavior).
    // The server rejects a quoted scalar (e.g. '2026-05-13' for a Date parameter), so these must not
    // be touched by the container-quoting fix.

    @Test(groups = {"unit"})
    public void testScalarDateParamIsNotQuoted() {
        Assert.assertEquals(HttpAPIClientHelper.formatStatementParam(LocalDate.of(2026, 5, 13)),
                "2026-05-13");
    }

    @Test(groups = {"unit"})
    public void testScalarStringAndNumberParamsAreUnchanged() {
        Assert.assertEquals(HttpAPIClientHelper.formatStatementParam("hello"), "hello");
        Assert.assertEquals(HttpAPIClientHelper.formatStatementParam(42), "42");
        Assert.assertEquals(HttpAPIClientHelper.formatStatementParam(new BigDecimal("1.50")), "1.50");
    }

    // --- Fix cases: container parameters single-quote their String/temporal leaves so the server's
    // Array/Map/Tuple text parser accepts them (previously emitted e.g. [2026-05-13] -> 400).

    @Test(groups = {"unit"})
    public void testArrayOfDatesQuotesElements() {
        Assert.assertEquals(
                HttpAPIClientHelper.formatStatementParam(
                        Arrays.asList(LocalDate.of(2026, 5, 13), LocalDate.of(2026, 5, 14))),
                "['2026-05-13','2026-05-14']");
    }

    @Test(groups = {"unit"})
    public void testArrayOfStringsQuotesElements() {
        Assert.assertEquals(
                HttpAPIClientHelper.formatStatementParam(Arrays.asList("a", "b")),
                "['a','b']");
    }

    @Test(groups = {"unit"})
    public void testArrayOfDateTimesQuotesElements() {
        Assert.assertEquals(
                HttpAPIClientHelper.formatStatementParam(
                        Collections.singletonList(LocalDateTime.of(2026, 5, 13, 16, 10, 0))),
                "['2026-05-13 16:10:00']");
    }

    @Test(groups = {"unit"})
    public void testObjectArrayOfDatesQuotesElements() {
        Assert.assertEquals(
                HttpAPIClientHelper.formatStatementParam(new LocalDate[]{LocalDate.of(2026, 5, 13)}),
                "['2026-05-13']");
    }

    @Test(groups = {"unit"})
    public void testNestedArrayOfDatesQuotesElements() {
        Assert.assertEquals(
                HttpAPIClientHelper.formatStatementParam(
                        Collections.singletonList(Collections.singletonList(LocalDate.of(2026, 5, 13)))),
                "[['2026-05-13']]");
    }

    @Test(groups = {"unit"})
    public void testMapWithDateValueQuotesKeyAndValue() {
        Assert.assertEquals(
                HttpAPIClientHelper.formatStatementParam(
                        Collections.singletonMap("k", LocalDate.of(2026, 5, 13))),
                "{'k':'2026-05-13'}");
    }

    @Test(groups = {"unit"})
    public void testArrayStringElementWithEmbeddedQuoteIsEscaped() {
        Assert.assertEquals(
                HttpAPIClientHelper.formatStatementParam(Collections.singletonList("a'b")),
                "['a\\'b']");
    }

    @Test(groups = {"unit"})
    public void testArrayNullElementRendersAsNull() {
        Assert.assertEquals(
                HttpAPIClientHelper.formatStatementParam(Arrays.asList(LocalDate.of(2026, 5, 13), null)),
                "['2026-05-13',NULL]");
    }

    // --- Contrast cases: numeric containers must stay UNQUOTED (quoting them causes the server to
    // reject the array, e.g. ['1','2'] for Array(Int32)).

    @Test(groups = {"unit"})
    public void testArrayOfIntegersIsNotQuoted() {
        Assert.assertEquals(
                HttpAPIClientHelper.formatStatementParam(Arrays.asList(1, 2, 3)),
                "[1,2,3]");
    }

    @Test(groups = {"unit"})
    public void testArrayOfDecimalsIsNotQuoted() {
        Assert.assertEquals(
                HttpAPIClientHelper.formatStatementParam(
                        Collections.singletonList(new BigDecimal("1.50"))),
                "[1.50]");
    }
}
