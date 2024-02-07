package com.clickhouse.client.gss;

import java.io.Serializable;
import java.util.Set;

import javax.security.auth.Subject;

import org.apache.hc.client5.http.utils.Base64;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;

public class GssAuthorizationContext implements Serializable {

    private static final String INTEGRATION_TEST_SNAME_PROP_KEY = "clickhouse.test.kerb.sname";
    private static final Logger log = LoggerFactory.getLogger(GssAuthorizationContext.class);

    private final Subject subject;

    private GssAuthorizationContext(Subject subject) {
        this.subject = subject;
    }

    public static GssAuthorizationContext initialize() {
        return new GssAuthorizationContext(SubjectProvider.getSubject());
    }

    public String getAuthToken(String user, String serverName, String host) throws GSSException {
        GSSCredential gssCredential = null;
        if (subject != null) {
            log.debug("Getting private credentials from subject");
            Set<GSSCredential> gssCreds = subject.getPrivateCredentials(GSSCredential.class);
            if (gssCreds != null && !gssCreds.isEmpty()) {
                gssCredential = gssCreds.iterator().next();
            }
        }

        GSSManager manager = GSSManager.getInstance();
        Oid desiredMech = getKerberosMech();
        if (gssCredential == null) {
            if (hasSpnegoSupport(manager)) {
                desiredMech = getSpnegoMech();
            }

            GSSName gssClientName = null;
            if (!ClickHouseDefaults.USER.getDefaultValue().equals(user)) {
                gssClientName = manager.createName(user, GSSName.NT_USER_NAME);
            } else {
                log.debug("GSS credential name ignored. User name is default");
            }
            gssCredential = manager.createCredential(gssClientName, 8 * 3600, desiredMech, GSSCredential.INITIATE_ONLY);
        }
        GSSName gssServerName = manager.createName(getSName(serverName, host), GSSName.NT_HOSTBASED_SERVICE);
        GSSContext secContext = manager.createContext(gssServerName, desiredMech, gssCredential,
                GSSContext.DEFAULT_LIFETIME);
        secContext.requestMutualAuth(true);
        return Base64.encodeBase64String(secContext.initSecContext(new byte[0], 0, 0));
    }

    private String getSName(String serverName, String host) {
        if (System.getProperty(INTEGRATION_TEST_SNAME_PROP_KEY) != null && isLocalhost(host)) {
            // integration test mode - it allows to integrate with servers
            // without editing /etc/hosts
            String serverNameIT = System.getProperty(INTEGRATION_TEST_SNAME_PROP_KEY);
            log.warn("Integration test mode. Using sname %s", serverNameIT);
            return serverNameIT;
        }
        return serverName + "@" + host;
    }

    private boolean isLocalhost(String host) {
        return "localhost".equals(host) || "127.0.0.1".equals(host);
    }

    private static Oid getSpnegoMech() throws GSSException {
        return new Oid("1.3.6.1.5.5.2");
    }

    private static Oid getKerberosMech() throws GSSException {
        return new Oid("1.2.840.113554.1.2.2");
    }

    private static boolean hasSpnegoSupport(GSSManager manager) throws GSSException {
        Oid spnego = getSpnegoMech();
        for (Oid mech : manager.getMechs()) {
            if (mech.equals(spnego)) {
                return true;
            }
        }
        return false;
    }
}
