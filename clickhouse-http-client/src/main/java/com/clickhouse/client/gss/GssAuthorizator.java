package com.clickhouse.client.gss;

import java.io.File;
import java.security.AccessController;
import java.util.Set;

import javax.security.auth.Subject;

import org.apache.hc.client5.http.utils.Base64;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

public class GssAuthorizator {

    private final String user;
    private final String serverName;
    private final String host;

    public GssAuthorizator(String user, String serverName, String host) throws GSSException {
        this.user = user;
        this.serverName = serverName;
        this.host = host;
    }

    public String getAuthToken() throws GSSException {
        GSSCredential gssCredential = null;
        Subject sub = Subject.getSubject(AccessController.getContext());
        if (sub != null) {
            Set<GSSCredential> gssCreds = sub.getPrivateCredentials(GSSCredential.class);
            if (gssCreds != null && !gssCreds.isEmpty()) {
                gssCredential = gssCreds.iterator().next();
            }
        }

        GSSManager manager = GSSManager.getInstance();
        GSSName gssServerName = manager.createName(serverName + "@" + host, GSSName.NT_HOSTBASED_SERVICE);
        Oid desiredMech = new Oid("1.2.840.113554.1.2.2");
        if (gssCredential == null) {
            desiredMech = new Oid("1.3.6.1.5.5.2");     // SPNEGO
            GSSName gssClientName = manager.createName(user, GSSName.NT_USER_NAME);
            gssCredential = manager.createCredential(gssClientName, 8 * 3600, desiredMech, GSSCredential.INITIATE_ONLY);
        }
        GSSContext secContext = manager.createContext(gssServerName, desiredMech, gssCredential, GSSContext.DEFAULT_LIFETIME);
        secContext.requestMutualAuth(true);
        return Base64.encodeBase64String(secContext.initSecContext(new byte[0], 0, 0));
    }
}
