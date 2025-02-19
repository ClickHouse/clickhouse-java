package com.clickhouse.data;

import java.io.Serializable;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Immutable ClickHouse version, which takes the form
 * {@code Year(Major).Feature(Minor).Maintenance(Patch).Build}. Prefix like 'v'
 * and suffix like '-[testing|stable|lts]' will be ignored in parsing and
 * comparison.
 */
@Deprecated
public final class ClickHouseVersion implements Comparable<ClickHouseVersion>, Serializable {
    private static final long serialVersionUID = 6721014333437055314L;

    private static final String STR_LATEST = "latest";

    private static final ClickHouseVersion defaultVersion = new ClickHouseVersion(false, 0, 0, 0, 0);
    private static final ClickHouseVersion latestVersion = new ClickHouseVersion(true, 0, 0, 0, 0);

    private static final ClickHouseCache<String, ClickHouseVersion> versionCache = ClickHouseCache.create(100, 300,
            ClickHouseVersion::parseVersion);

    @SuppressWarnings({ "squid:S5843", "squid:S5857" })
    private static final Pattern versionPattern = Pattern.compile(
            "^(?:.*?[\\s:])?(\\d+)(?:\\s*\\.\\s*(\\d+))?(?:\\s*\\.\\s*(\\d+))?(?:\\s*\\.\\s*(\\d+))?(?:|[\\.\\s].*)",
            Pattern.DOTALL);

    /**
     * Checks if the version is valid according to the given maven-like version
     * range. For examples:
     * <ul>
     * <li><code>21.3</code> 21.3.x.x, short version of [21.3,21.4)</li>
     * <li><code>[21.3,21.4)</code> 21.3.x.x (included) to 21.4.x.x (not
     * included)</li>
     * <li><code>[21.3,21.4]</code> 21.3.x.x to 21.4.x.x (both included)</li>
     * <li><code>[21.3,)</code> 21.3.x.x or higher</li>
     * <li><code>(,21.3],[21.8,)</code> to 21.3.x.x (included) and 21.8.x.x or
     * higher</li>
     * </ul>
     * 
     * @param version version, null is treated as {@code 0.0.0.0}
     * @param range   maven-like version range, null or empty means always invalid
     * @return true if the version is valid; false otherwise
     */
    public static boolean check(String version, String range) {
        return ClickHouseVersion.of(version).check(range);
    }

    /**
     * Compares two versions part by part, which is not semantical. For example:
     * {@code compare("21.3.1", "21.3") > 0}, because "21.3.1.0" is greater than
     * "21.3.0.0". However, {@code check("21.3.1", "(,21.3]") == true}, since
     * "21.3.1" is considered as part of "21.3" series.
     *
     * @param fromVersion version
     * @param toVersion   version to compare with
     * @return positive integer if {@code fromVersion} is newer than
     *         {@code toVersion}; zero if they're equal; or negative integer if
     *         {@code fromVersion} is older
     */
    public static int compare(String fromVersion, String toVersion) {
        return ClickHouseVersion.of(fromVersion).compareTo(ClickHouseVersion.of(toVersion));
    }

    /**
     * Parses the given string to extract version. Behind the scene, cache is used
     * to avoid unnecessary overhead.
     *
     * @param version version, null or empty string is treated as {@code 0.0.0.0}
     * @return parsed version
     */
    public static ClickHouseVersion of(String version) {
        if (version == null || version.isEmpty()) {
            return defaultVersion;
        }

        return versionCache.get(version);
    }

    /**
     * Creates a new version object using given numbers.
     *
     * @param yearOrMajor year or major vrsion
     * @param more        more version numbers if any
     * @return version
     */
    public static ClickHouseVersion of(int yearOrMajor, int... more) {
        int len = more != null ? more.length : 0;
        return new ClickHouseVersion(false, yearOrMajor, len > 0 ? more[0] : 0,
                len > 1 ? more[1] : 0, len > 2 ? more[2] : 0); // NOSONAR
    }

    /**
     * Parses given version without caching.
     * 
     * @param version version, null or empty string is treated as {@code 0.0.0.0}
     * @return parsed version
     */
    protected static ClickHouseVersion parseVersion(String version) {
        if (version == null || version.isEmpty()) {
            return defaultVersion;
        }

        boolean latest = false;
        int[] parts = new int[4];
        Matcher m = versionPattern.matcher(version);
        if (m.matches()) {
            for (int i = 0, len = Math.min(m.groupCount(), parts.length); i < len; i++) {
                try {
                    parts[i] = Integer.parseInt(m.group(i + 1));
                } catch (NumberFormatException e) {
                    // definitely don't want to break anything
                }
            }
        } else {
            version = version.trim().toLowerCase(Locale.ROOT);

            int index = Math.max(version.lastIndexOf(' '), version.lastIndexOf(':'));
            latest = STR_LATEST.equals(index == -1 ? version : version.substring(index + 1));
        }

        return latest ? latestVersion : new ClickHouseVersion(false, parts[0], parts[1], parts[2], parts[3]);
    }

    private final boolean latest;

    private final int year; // major
    private final int feature; // minor
    private final int maintenance; // patch
    private final int build;

    protected ClickHouseVersion(boolean latest, int year, int feature, int maintenance, int build) {
        this.latest = latest;

        if (latest) {
            this.year = 0;
            this.feature = 0;
            this.maintenance = 0;
            this.build = 0;
        } else {
            this.year = year > 0 ? year : 0;
            this.feature = feature > 0 ? feature : 0;
            this.maintenance = maintenance > 0 ? maintenance : 0;
            this.build = build > 0 ? build : 0;
        }
    }

    /**
     * Compares current version and the given one. When {@code includeEmptyParts} is
     * {@code true}, this method returns 0(instead of 1) when comparing '21.3.1.2'
     * with '21.3', because they're in the same series of '21.3'.
     * 
     * @param o                    the object to be compared
     * @param sameSeriesComparison whether compare if two version are in same series
     * @return a negative integer, zero, or a positive integer as this object is
     *         less than, equal to, or greater than the specified object
     */
    protected int compareTo(ClickHouseVersion o, boolean sameSeriesComparison) {
        if (o == null) {
            o = defaultVersion;
        }

        if (this == o) {
            return 0;
        }

        if (latest) {
            return o.latest ? 0 : 1;
        } else if (o.latest) {
            return -1;
        }

        int result = year - o.year;
        if (result != 0) {
            return result;
        } else if (sameSeriesComparison && o.feature == 0 && o.maintenance == 0 && o.build == 0) {
            return 0;
        }

        result = feature - o.feature;
        if (result != 0) {
            return result;
        } else if (sameSeriesComparison && o.maintenance == 0 && o.build == 0) {
            return 0;
        }

        result = maintenance - o.maintenance;
        if (result != 0) {
            return result;
        } else if (sameSeriesComparison && o.build == 0) {
            return 0;
        }

        return build - o.build;
    }

    /**
     * Checks if the version belongs to the given series. For example: 21.3.1.1
     * belongs to 21.3 series but not 21.4 or 21.3.2.
     *
     * @param version version series, null will be treated as {@code 0.0.0.0}
     * @return true if the version belongs to the given series; false otherwise
     */
    public boolean belongsTo(ClickHouseVersion version) {
        if (version == null) {
            version = defaultVersion;
        }

        if (isLatest()) {
            return version.isLatest();
        } else if (version.isLatest()) {
            return false;
        }

        if (year != version.year) {
            return false;
        }

        if (version.feature == 0 && version.maintenance == 0 && version.build == 0) {
            return true;
        } else if (feature != version.feature) {
            return false;
        }

        if (version.maintenance == 0 && version.build == 0) {
            return true;
        } else if (maintenance != version.maintenance) {
            return false;
        }

        return version.build == 0 || build == version.build;
    }

    /**
     * Checks if the version belongs to the given series. For example: 21.3.1.1
     * belongs to 21.3 series but not 21.4 or 21.3.2.
     *
     * @param version version series
     * @return true if the version belongs to the given series; false otherwise
     */
    public boolean belongsTo(String version) {
        return belongsTo(ClickHouseVersion.of(version));
    }

    /**
     * Checks if the version is latest or not.
     *
     * @return true if it's latest; false otherwise
     */
    public boolean isLatest() {
        return latest;
    }

    /**
     * Gets year number.
     *
     * @return year number
     */
    public int getYear() {
        return year;
    }

    /**
     * Gets feature release.
     *
     * @return feature release
     */
    public int getFeatureRelease() {
        return feature;
    }

    /**
     * Gets maintenance release.
     *
     * @return maintenance release
     */
    public int getMaintenanceRelease() {
        return maintenance;
    }

    /**
     * Gets build number.
     *
     * @return build number
     */
    public int getBuilderNumber() {
        return build;
    }

    /**
     * Gets major version in semantic versioning. Same as {@link #getYear()}.
     *
     * @return major version
     */
    public int getMajorVersion() {
        return getYear();
    }

    /**
     * Gets minor version in semantic versioning. Same as
     * {@link #getFeatureRelease()}.
     *
     * @return minor version
     */
    public int getMinorVersion() {
        return getFeatureRelease();
    }

    /**
     * Gets patch in semantic versioning. Same a {@link #getMaintenanceRelease()}.
     *
     * @return patch
     */
    public int getPatch() {
        return getMaintenanceRelease();
    }

    /**
     * Checks if the version is newer or equal to the given one. Pay attention that
     * when comparing "21.3.1.1" to "21.3", {@link #compareTo(ClickHouseVersion)}
     * says the former is greater, but this method will return false(because
     * 21.3.x.x still belongs to 21.3 series).
     *
     * @param version version to compare
     * @return true if the version is newer or equal to the given one; false
     *         otherwise
     */
    public boolean isNewerOrEqualTo(ClickHouseVersion version) {
        return compareTo(version, true) >= 0;
    }

    /**
     * Checks if the version is newer or equal to the given one. Pay attention that
     * when comparing "21.3.1.1" to "21.3", {@link #compareTo(ClickHouseVersion)}
     * says the former is greater, but this method will return false(because
     * 21.3.x.x still belongs to 21.3 series).
     *
     * @param version version to compare
     * @return true if the version is newer or equal to the given one; false
     *         otherwise
     */
    public boolean isNewerOrEqualTo(String version) {
        return isNewerOrEqualTo(ClickHouseVersion.of(version));
    }

    /**
     * Checks if the version is newer than the given one. Same as
     * {@code compareTo(version) > 0}.
     *
     * @param version version to compare
     * @return true if the version is newer than the given one; false otherwise
     */
    public boolean isNewerThan(ClickHouseVersion version) {
        return compareTo(version, false) > 0;
    }

    /**
     * Checks if the version is newer than the given one. Same as
     * {@code compareTo(version) > 0}.
     *
     * @param version version to compare
     * @return true if the version is newer than the given one; false otherwise
     */
    public boolean isNewerThan(String version) {
        return isNewerThan(ClickHouseVersion.of(version));
    }

    /**
     * Checks if the version is older or equal to the given one. Pay attention that
     * when comparing "21.3.1.1" to "21.3", {@link #compareTo(ClickHouseVersion)}
     * says the former is greater, but this method will return true(because 21.3.x.x
     * still belongs to 21.3 series).
     *
     * @param version version to compare
     * @return true if the version is older or equal to the given one; false
     *         otherwise
     */
    public boolean isOlderOrEqualTo(ClickHouseVersion version) {
        return compareTo(version, true) <= 0;
    }

    /**
     * Checks if the version is older or equal to the given one. Pay attention that
     * when comparing "21.3.1.1" to "21.3", {@link #compareTo(ClickHouseVersion)}
     * says the former is greater, but this method will return true(because 21.3.x.x
     * still belongs to 21.3 series).
     *
     * @param version version to compare
     * @return true if the version is older or equal to the given one; false
     *         otherwise
     */
    public boolean isOlderOrEqualTo(String version) {
        return isOlderOrEqualTo(ClickHouseVersion.of(version));
    }

    /**
     * Checks if the version is older than the given one. Same as
     * {@code compareTo(version) < 0}.
     *
     * @param version version to compare
     * @return true if the version is older than the given one; false otherwise
     */
    public boolean isOlderThan(ClickHouseVersion version) {
        return compareTo(version, false) < 0;
    }

    /**
     * Checks if the version is older than the given one. Same as
     * {@code compareTo(version) < 0}.
     *
     * @param version version to compare
     * @return true if the version is older than the given one; false otherwise
     */
    public boolean isOlderThan(String version) {
        return isOlderThan(ClickHouseVersion.of(version));
    }

    /**
     * Checks if the version is valid according to the given maven-like version
     * range.
     * 
     * @param range version range, null or empty string means always invalid
     * @return true if the version is valid; false otherwise
     */
    public boolean check(String range) {
        if (range == null || range.isEmpty()) {
            return false;
        }

        boolean result = false;
        StringBuilder builder = new StringBuilder();
        for (int i = 0, len = range.length(); i < len; i++) {
            char ch = range.charAt(i);
            if ((ch >= '0' && ch <= '9') || ch == '.') {
                builder.append(ch);
            } else if (ch == '[' || ch == '(') {
                if (builder.length() > 0) {
                    throw new IllegalArgumentException(String.format(Locale.ROOT,
                            "Expect a comma after %s at %d but got %s", builder.toString(), i, ch));
                }

                ClickHouseVersion v1 = null;
                ClickHouseVersion v2 = null;

                char nextCh = '\0';
                for (i = i + 1; i < len; i++) {
                    nextCh = range.charAt(i);
                    if ((nextCh >= '0' && nextCh <= '9') || nextCh == '.') {
                        builder.append(nextCh);
                    } else if (nextCh == ',') {
                        v1 = ClickHouseVersion.of(builder.toString());
                        builder.setLength(0);
                    } else if (nextCh == ')' || nextCh == ']') {
                        if (builder.length() == 0) {
                            v2 = latestVersion;
                        } else {
                            v2 = ClickHouseVersion.of(builder.toString());
                            builder.setLength(0);
                        }
                        break;
                    }
                }

                if (v1 == null || v2 == null) {
                    throw new IllegalArgumentException(
                            "Brackets must come in pairs and at least one version and a comma must be specified within brackets");
                }

                result = ch == '(' ? isNewerThan(v1) : isNewerOrEqualTo(v1);
                if (!result) {
                    break;
                }

                result = nextCh == ')' ? (latest || isOlderThan(v2)) : isOlderOrEqualTo(v2);
                if (!result) {
                    break;
                }
            } else if (ch == ',' && builder.length() > 0) {
                ClickHouseVersion v = of(builder.toString());
                builder.setLength(0);

                result = belongsTo(v);
                if (!result) {
                    break;
                }
            }
        }

        if (builder.length() > 0) {
            result = belongsTo(builder.toString());
        }

        return result;
    }

    @Override
    public int compareTo(ClickHouseVersion o) {
        return compareTo(o, false);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ClickHouseVersion other = (ClickHouseVersion) obj;
        return (latest && other.latest) || latest == other.latest && year == other.year && feature == other.feature
                && maintenance == other.maintenance && build == other.build;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (latest ? 1231 : 1237);
        result = prime * result + year;
        result = prime * result + feature;
        result = prime * result + maintenance;
        result = prime * result + build;
        return result;
    }

    @Override
    public String toString() {
        return isLatest() ? STR_LATEST
                : new StringBuilder().append(year).append('.').append(feature).append('.').append(maintenance)
                        .append('.').append(build).toString();
    }
}
