package com.clickhouse.client.api;

import com.clickhouse.client.api.http.ClickHouseHttpProto;
import com.clickhouse.client.api.internal.ValidationUtils;

import java.util.Map;

/**
 * Reusable ClickHouse session configuration that can be applied to clients or operation settings.
 */
public class Session {
    private String sessionId;
    private Boolean sessionCheck;
    private Integer sessionTimeout;
    private String sessionTimezone;

    static Session extractFrom(Map<String, Object> configuration) {
        Session session = new Session();

        String sessionId = (String) configuration.get(ClientConfigProperties.serverSetting(ClickHouseHttpProto.QPARAM_SESSION_ID));
        if (sessionId != null) {
            session.setSessionId(sessionId);
        }

        String sessionCheck = (String) configuration.get(ClientConfigProperties.serverSetting(ClickHouseHttpProto.QPARAM_SESSION_CHECK));
        if (sessionCheck != null) {
            session.setSessionCheck("1".equals(sessionCheck) || Boolean.parseBoolean(sessionCheck));
        }

        String sessionTimeout = (String) configuration.get(ClientConfigProperties.serverSetting(ClickHouseHttpProto.QPARAM_SESSION_TIMEOUT));
        if (sessionTimeout != null) {
            session.setSessionTimeout(Integer.parseInt(sessionTimeout));
        }

        String sessionTimezone = (String) configuration.get(ClientConfigProperties.serverSetting(ClickHouseHttpProto.QPARAM_SESSION_TIMEZONE));
        if (sessionTimezone != null) {
            session.setSessionTimezone(sessionTimezone);
        }

        return session;
    }

    public synchronized Session setSessionId(String sessionId) {
        ValidationUtils.checkNonBlank(sessionId, ClickHouseHttpProto.QPARAM_SESSION_ID);
        this.sessionId = sessionId;
        return this;
    }

    public synchronized String getSessionId() {
        return sessionId;
    }

    public synchronized Session setSessionCheck(boolean sessionCheck) {
        this.sessionCheck = sessionCheck;
        return this;
    }

    public synchronized Boolean getSessionCheck() {
        return sessionCheck;
    }

    public synchronized Session setSessionTimeout(int timeoutInSeconds) {
        ValidationUtils.checkPositive(timeoutInSeconds, ClickHouseHttpProto.QPARAM_SESSION_TIMEOUT);
        this.sessionTimeout = timeoutInSeconds;
        return this;
    }

    public synchronized Integer getSessionTimeout() {
        return sessionTimeout;
    }

    public synchronized Session setSessionTimezone(String timezone) {
        ValidationUtils.checkNonBlank(timezone, ClickHouseHttpProto.QPARAM_SESSION_TIMEZONE);
        this.sessionTimezone = timezone;
        return this;
    }

    public synchronized String getSessionTimezone() {
        return sessionTimezone;
    }

    public synchronized void updateSessionId(String sessionId) {
        setSessionId(sessionId);
    }

    public synchronized void applyTo(Map<String, Object> requestSettings) {
        putIfSet(requestSettings, ClickHouseHttpProto.QPARAM_SESSION_ID, sessionId);
        putIfSet(requestSettings, ClickHouseHttpProto.QPARAM_SESSION_CHECK,
                sessionCheck == null ? null : (sessionCheck ? "1" : "0"));
        putIfSet(requestSettings, ClickHouseHttpProto.QPARAM_SESSION_TIMEOUT,
                sessionTimeout == null ? null : String.valueOf(sessionTimeout));
        putIfSet(requestSettings, ClickHouseHttpProto.QPARAM_SESSION_TIMEZONE, sessionTimezone);
    }

    private static void putIfSet(Map<String, Object> settings, String key, String value) {
        if (value != null) {
            settings.put(ClientConfigProperties.serverSetting(key), value);
        }
    }
}
