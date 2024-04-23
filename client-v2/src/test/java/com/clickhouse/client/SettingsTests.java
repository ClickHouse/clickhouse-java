package com.clickhouse.client;

import com.clickhouse.client.api.internal.ValidationUtils;
import com.clickhouse.client.api.query.QuerySettings;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SettingsTests {


    @Test
    public void testQuerySettings() {
        QuerySettings settings = new QuerySettings();

        Assert.expectThrows(ValidationUtils.SettingsValidationException.class, () -> {
            settings.setFormat("YAML");
        });

        Assert.expectThrows(ValidationUtils.SettingsValidationException.class, () -> {
            settings.setCompressAlgorithm("TAR");
        });


    }
}
