package com.clickhouse.client.api;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Short description of a configuration property for users. Each constant of {@link ClientConfigProperties} and of
 * {@code com.clickhouse.jdbc.DriverProperties} has one.
 * <p>
 * At build time, an annotation processor generates the class {@code <Enum>Descriptions} next to each enum with such
 * constants, for example {@code ClientConfigPropertiesDescriptions}. Its method {@code getDescriptions()} returns a
 * map from each constant to its description. A constant of such an enum that has no description, or has a blank
 * description, fails the build.
 */
@Documented
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.FIELD)
public @interface Description {

    /**
     * Returns the description.
     *
     * @return the description, not blank
     */
    String value();
}
