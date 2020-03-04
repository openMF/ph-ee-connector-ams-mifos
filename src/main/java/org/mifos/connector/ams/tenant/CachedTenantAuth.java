package org.mifos.connector.ams.tenant;

import java.util.Date;

public class CachedTenantAuth {

    private String token;
    private Date accessTokenExpiration;

    public CachedTenantAuth() {
    }

    public CachedTenantAuth(String token, Date accessTokenExpiration) {
        this.token = token;
        this.accessTokenExpiration = accessTokenExpiration;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public Date getAccessTokenExpiration() {
        return accessTokenExpiration;
    }

    public void setAccessTokenExpiration(Date accessTokenExpiration) {
        this.accessTokenExpiration = accessTokenExpiration;
    }
}
