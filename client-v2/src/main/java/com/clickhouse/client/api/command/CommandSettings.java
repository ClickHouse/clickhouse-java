package com.clickhouse.client.api.command;

import com.clickhouse.client.api.Session;
import com.clickhouse.client.api.query.QuerySettings;

public class CommandSettings extends QuerySettings {
    @Override
    public CommandSettings setSessionId(String sessionId) {
        super.setSessionId(sessionId);
        return this;
    }

    @Override
    public CommandSettings setSessionCheck(boolean sessionCheck) {
        super.setSessionCheck(sessionCheck);
        return this;
    }

    @Override
    public CommandSettings setSessionTimeout(int timeoutInSeconds) {
        super.setSessionTimeout(timeoutInSeconds);
        return this;
    }

    @Override
    public CommandSettings setSessionTimezone(String timezone) {
        super.setSessionTimezone(timezone);
        return this;
    }

    public CommandSettings clearSession() {
        super.clearSession();
        return this;
    }

    @Override
    public CommandSettings use(Session session) {
        super.use(session);
        return this;
    }
}
