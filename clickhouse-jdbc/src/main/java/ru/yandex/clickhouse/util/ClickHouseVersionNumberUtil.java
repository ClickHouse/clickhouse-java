package ru.yandex.clickhouse.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Very simple version number parser. It is only needed for ClickHouse driver
 * and database version numbers
 * 
 * @deprecated As of release 0.3.2, replaced by
 *             {@link com.clickhouse.client.ClickHouseVersion} and it will be
 *             removed in 0.3.3
 */
@Deprecated
public final class ClickHouseVersionNumberUtil {

    private static final Pattern VERSION_NUMBER_PATTERN = Pattern.compile("^\\s*(\\d+)\\.(\\d+).*");
    private static final Pattern NON_NUMBERIC_PATTERN = Pattern.compile("[^0-9.]");

    public static int getMajorVersion(String versionNumber) {
        Matcher m = VERSION_NUMBER_PATTERN.matcher(versionNumber);
        return m.matches() ? Integer.parseInt(m.group(1)) : 0;
    }

    public static int getMinorVersion(String versionNumber) {
        Matcher m = VERSION_NUMBER_PATTERN.matcher(versionNumber);
        return m.matches() ? Integer.parseInt(m.group(2)) : 0;
    }

    public static int compare(String currentVersion, String targetVersion) {
        if (currentVersion == null || targetVersion == null || currentVersion.isEmpty() || targetVersion.isEmpty()) {
            throw new IllegalArgumentException("Both version cannot be null or empty");
        }

        currentVersion = NON_NUMBERIC_PATTERN.matcher(currentVersion).replaceAll("");
        targetVersion = NON_NUMBERIC_PATTERN.matcher(targetVersion).replaceAll("");
        if (currentVersion.equals(targetVersion)) {
            return 0;
        }

        String[] v1 = currentVersion.split("\\.");
        String[] v2 = targetVersion.split("\\.");

        int result = 0;
        for (int i = 0, len = Math.min(v1.length, v2.length); i < len; i++) {
            int n1 = Integer.parseInt(v1[i]);
            int n2 = Integer.parseInt(v2[i]);

            if (n1 == n2) {
                continue;
            } else {
                result = n1 > n2 ? 1 : -1;
                break;
            }
        }

        return result;
    }

    private ClickHouseVersionNumberUtil() {
        /* do not instantiate util */ }
}
