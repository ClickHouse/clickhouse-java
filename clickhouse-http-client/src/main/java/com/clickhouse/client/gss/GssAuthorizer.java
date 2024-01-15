package com.clickhouse.client.gss;

import java.util.function.Function;

import org.apache.hc.client5.http.utils.Base64;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

public class GssAuthorizer {

    private final GSSContext clientContext;
    private byte[] inToken;
    private String outTokenEncoded;

    public GssAuthorizer(String serverName, String host) throws GSSException {
        GSSManager manager = GSSManager.getInstance();
        GSSName gssName = manager.createName(serverName + "@" + host, GSSName.NT_HOSTBASED_SERVICE);
        Oid krb5SpnegoOid = new Oid("1.3.6.1.5.5.2");
        this.clientContext = manager.createContext(gssName, krb5SpnegoOid, null, GSSContext.DEFAULT_LIFETIME);
    }

    public boolean isEstablished() {
        return clientContext.isEstablished();
    }

    public void negotiate(Function<String, String> negotiateTokenFun) throws GSSException {
        byte[] outToken = clientContext.initSecContext(inToken, 0, inToken.length);
        outTokenEncoded = Base64.encodeBase64String(outToken);
        String inTokenEncoded = negotiateTokenFun.apply(outTokenEncoded);
        inToken = Base64.decodeBase64(inTokenEncoded);
    }

    public String getToken() {
        if (isEstablished()) {
            return outTokenEncoded;
        }
        throw new IllegalStateException("Token not initialized");
    }
}
