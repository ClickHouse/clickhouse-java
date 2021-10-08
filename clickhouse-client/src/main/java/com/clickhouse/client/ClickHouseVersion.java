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

    private static final String STR_LATEST = "latest";
    private static final ClickHouseVersion defaultVersion = new ClickHouseVersion(false, 0, 0, 0, 0);
    private static final ClickHouseVersion latestVersion = new ClickHouseVersion(true, 0, 0, 0, 0);

    @SuppressWarnings({ "squid:S5843", "squid:S5857" })
    private static final Pattern versionPattern = Pattern.compile(
            "^(?:.*?[\\s:])?(\\d+)(?:\\s*\\.\\s*(\\d+))?(?:\\s*\\.\\s*(\\d+))?(?:\\s*\\.\\s*(\\d+))?(?:|[\\.\\s].*)",
            Pattern.DOTALL);

    /**
     * Compares two versions.
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
     * Parses given version in string format.
     *
     * @param version version, null or empty string is treated as {@code 0.0.0.0}
     * @return parsed version
     */
    public static ClickHouseVersion of(String version) {
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
            version = version.trim().toLowerCase();

            int index = Math.max(version.lastIndexOf(' '), version.lastIndexOf(':'));
            latest = STR_LATEST.equals(index == -1 ? version : version.substring(index + 1));
        }

        return latest ? latestVersion : new ClickHouseVersion(false, parts[0], parts[1], parts[2], parts[3]);
    }

    private final boolean latest;

    private final int year;
    private final int major;
    private final int minor;
    private final int internal; // or patch?

    // Active Releases:
    // https://github.com/ClickHouse/ClickHouse/pulls?q=is%3Aopen+is%3Apr+label%3Arelease
    protected ClickHouseVersion(boolean latest, int year, int major, int minor, int internal) {
        this.latest = latest;

        if (latest) {
            this.year = 0;
            this.major = 0;
            this.minor = 0;
            this.internal = 0;
        } else {
            this.year = year > 0 ? year : 0;
            this.major = major > 0 ? major : 0;
            this.minor = minor > 0 ? minor : 0;
            this.internal = internal > 0 ? internal : 0;
        }
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
     * Gets major/feature version.
     *
     * @return major/feature version
     */
    public int getMajor() {
        return major;
    }

    /**
     * Gets minor version.
     *
     * @return minor version
     */
    public int getMinor() {
        return minor;
    }

    /**
     * Gets internal build number.
     *
     * @return internal build number
     */
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
    public boolean isNewerOrEqualTo(String version) {
        return compareTo(ClickHouseVersion.of(version)) >= 0;
    }

    /**
     * Checks if current version is newer than the given version.
     *
     * @param version version to compare
     * @return true if current version is newer than the given one; false otherwise
     */
    public boolean isNewerThan(String version) {
        return compareTo(ClickHouseVersion.of(version)) > 0;
    }

    /**
     * Checks if current version is older or equal to the given version.
     *
     * @param version version to compare
     * @return true if current version is older or equal to the given one; false
     *         otherwise
     */
    public boolean isOlderOrEqualTo(String version) {
        return compareTo(ClickHouseVersion.of(version)) <= 0;
    }

    /**
     * Checks if current version is older than the given version.
     *
     * @param version version to compare
     * @return true if current version is older than the given one; false otherwise
     */
    public boolean isOlderThan(String version) {
        return compareTo(ClickHouseVersion.of(version)) < 0;
    }

    /**
     * Checks if this version belongs to the given series. For example: 21.3.1
     * belongs to 21.3 but not 21.6 or 21.3.2. No version except {@code latest}
     * belongs to {@code latest} and vice versa.
     * 
     * @param version version series
     * @return true if this version belongs to the given series; false otherwise
     */
    public boolean belongsTo(String version) {
        return belongsTo(ClickHouseVersion.of(version));
    }

    /**
     * Checks if this version belongs to the given series. For example: 21.3.1
     * belongs to 21.3 but not 21.6 or 21.3.2. No version except {@code latest}
     * belongs to {@code latest} and vice versa.
     * 
     * @param version version series, null is treated as {@code 0.0.0.0}
     * @return true if this version belongs to the given series; false otherwise
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

        if (version.major == 0) {
            return true;
        } else if (major != version.major) {
            return false;
        }

        if (version.minor == 0) {
            return true;
        } else if (minor != version.minor) {
            return false;
        }

        return version.internal == 0 || internal == version.internal;
    }

    /**
     * Checks if this version is beyond the given series. For example: 21.3.1 is
     * beyond 21.2, but not 21.3 or 21.3.1(because they all belong to 21.3 series).
     * No version is beyond {@code latest} but it's beyond all other versions.
     * 
     * @param version version series
     * @return true if this version belongs to the given series; false otherwise
     */
    public boolean isBeyond(String version) {
        return isBeyond(ClickHouseVersion.of(version));
    }

    /**
     * Checks if this version is beyond the given series. For example: 21.3.1 is
     * beyond 21.2, but not 21.3 or 21.3.1(because they all belong to 21.3 series).
     * No version is beyond {@code latest} but it's beyond all other versions.
     * 
     * @param version version series, null is treated as {@code 0.0.0.0}
     * @return true if this version is beyond the given series; false otherwise
     */
    public boolean isBeyond(ClickHouseVersion version) {
        if (version == null) {
            version = defaultVersion;
        }

        if (isLatest()) {
            return !version.isLatest();
        } else if (version.isLatest()) {
            return false;
        }

        int result = year - version.year;
        if (result != 0) {
            return result > 0;
        } else if (version.major == 0 && version.minor == 0 && version.internal == 0) {
            return false;
        }

        result = major - version.major;
        if (result != 0) {
            return result > 0;
        } else if (version.minor == 0 && version.internal == 0) {
            return false;
        }

        result = minor - version.minor;
        if (result != 0) {
            return result > 0;
        } else if (version.internal == 0) {
            return false;
        }

        return internal > version.internal;
    }

    /**
     * Checks if this version is older than or belongs to the given version.
     *
     * @param version version
     * @return true if this version is older than or belongs to the given version;
     *         false otherwise
     */
    public boolean isOlderOrBelongsTo(String version) {
        ClickHouseVersion theOther = ClickHouseVersion.of(version);
        return compareTo(theOther) < 0 || belongsTo(theOther);
    }

    @Override
    public int compareTo(ClickHouseVersion o) {
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
        return (latest && other.latest) || latest == other.latest && year == other.year && major == other.major
                && minor == other.minor && internal == other.internal;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (latest ? 1231 : 1237);
        result = prime * result + year;
        result = prime * result + major;
        result = prime * result + minor;
        result = prime * result + internal;
        return result;
    }

    @Override
    public String toString() {
        return isLatest() ? STR_LATEST
                : new StringBuilder().append(year).append('.').append(major).append('.').append(minor).append('.')
                        .append(internal).toString();
    }
}
