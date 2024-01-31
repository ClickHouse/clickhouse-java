package com.clickhouse.client.gss;

import java.security.AccessController;

import javax.security.auth.Subject;

class SubjectProvider {

    static Subject getSubject() {
        return Subject.getSubject(AccessController.getContext());        // TODO add handling for java 18+
    }

}
