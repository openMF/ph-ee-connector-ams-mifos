package org.mifos.connector.ams.tenant;

import java.util.Date;
import java.util.Objects;

public class CachedTenantAuth {

    private String token;
    private Date accessTokenExpiration;

    public CachedTenantAuth(String token, Date accessTokenExpiration) {
        this.token = token;
        this.accessTokenExpiration = accessTokenExpiration;
    }

    public String getToken() {
        return token;
    }

    public Date getAccessTokenExpiration() {
        return accessTokenExpiration;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CachedTenantAuth that = (CachedTenantAuth) o;
        return Objects.equals(token, that.token) && Objects.equals(accessTokenExpiration, that.accessTokenExpiration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(token, accessTokenExpiration);
    }
}
