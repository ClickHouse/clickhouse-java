package com.clickhouse.client;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ClickHouse server version number takes the form
 * {@code Year.Major.Minor.Internal}. Prefix like 'v' and suffix like
 * '-[testing|stable|lts]' will be ignored in parsing and comparison.
 */
public final class ClickHouseVersion implements Comparable<ClickHouseVersion>, Serializable {
    private static final long serialVersionUID = 6721014333437055314L;

    private static final ClickHouseVersion defaultVersion = new ClickHouseVersion(0, 0, 0, 0);

    @SuppressWarnings({ "squid:S5843", "squid:S5857" })
    private static final Pattern versionPattern = Pattern.compile(
            "^(?:.*?\\s)?(\\d+)(?:\\s*\\.\\s*(\\d+))?(?:\\s*\\.\\s*(\\d+))?(?:\\s*\\.\\s*(\\d+))?(?:|[\\.\\s].*)",
            Pattern.DOTALL);

    /**
     * Compare two versions.
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
     * Parse given version number in string format.
     *
     * @param version version number
     * @return parsed version
     */
    public static ClickHouseVersion of(String version) {
        if (version == null || version.isEmpty()) {
            return defaultVersion;
        }

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
        }

        return new ClickHouseVersion(parts[0], parts[1], parts[2], parts[3]);
    }

    private final int year;
    private final int major;
    private final int minor;
    private final int internal; // or patch?

    // Active Releases:
    // https://github.com/ClickHouse/ClickHouse/pulls?q=is%3Aopen+is%3Apr+label%3Arelease
    protected ClickHouseVersion(int year, int major, int minor, int internal) {
        this.year = year > 0 ? year : 0;
        this.major = major > 0 ? major : 0;
        this.minor = minor > 0 ? minor : 0;
        this.internal = internal > 0 ? internal : 0;
    }

    public int getYear() {
        return year;
    }

    public int getMajor() {
        return major;
    }

    public int getMinor() {
        return minor;
    }

    public int getInternal() {
        return internal;
    }

    /**
     * Checks if current version is newer or equal to the given version.
     *
     * @param version version to compare
     * @return true if current version is newer or equal to the given one; false
     *         otherwise
     */
    public boolean isNewerOrEqual(String version) {
        return compareTo(ClickHouseVersion.of(version)) >= 0;
    }

    @Override
    public int compareTo(ClickHouseVersion o) {
        if (this == o) {
            return 0;
        }

        int result = year - o.year;
        if (result != 0) {
            return result;
        }

        result = major - o.major;
        if (result != 0) {
            return result;
        }

        result = minor - o.minor;
        if (result != 0) {
            return result;
        }

        return internal - o.internal;
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
        return year == other.year && major == other.major && minor == other.minor && internal == other.internal;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + year;
        result = prime * result + major;
        result = prime * result + minor;
        result = prime * result + internal;
        return result;
    }

    @Override
    public String toString() {
        return new StringBuilder().append(year).append('.').append(major).append('.').append(minor).append('.')
                .append(internal).toString();
    }
}
