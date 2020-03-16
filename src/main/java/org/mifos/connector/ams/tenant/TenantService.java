package org.mifos.connector.ams.tenant;

import org.mifos.connector.ams.properties.Tenant;
import org.mifos.connector.ams.properties.TenantProperties;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.ProducerTemplate;
import org.eclipse.jetty.http.HttpHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
@ConditionalOnExpression("${ams.local.enabled}")
public class TenantService {

    @Autowired
    private ProducerTemplate producerTemplate;

    @Autowired
    private TenantProperties tenantProperties;

    private final Map<String, CachedTenantAuth> cachedTenantAuths = new ConcurrentHashMap<>();

    @PostConstruct
    public void setup() {
        tenantProperties.getTenants().forEach(tenant -> {
            cachedTenantAuths.put(tenant.getName(), new CachedTenantAuth());
        });
    }

    public Map<String, Object> getHeaders(String tenantName, boolean isAuthNeeded) {
        Tenant tenant = tenantProperties.getTenant(tenantName);
        if (tenant == null) {
            throw new RuntimeException(String.format("Could not get headers for tenant: %s, no application configuration found!", tenantName));
        }

        Map<String, Object> headers = new HashMap<>();
        if(isAuthNeeded) {
            headers.put(HttpHeader.AUTHORIZATION.asString(), getTenantAuthData(tenant).getToken());
        }
        headers.put("Fineract-Platform-TenantId", tenantName);

        return headers;
    }

    private CachedTenantAuth getTenantAuthData(Tenant tenant) {
        CachedTenantAuth cachedTenantAuth = cachedTenantAuths.get(tenant.getName());
        if (cachedTenantAuth.getToken() == null || isAccessTokenExpired(cachedTenantAuth.getAccessTokenExpiration())) {
            cachedTenantAuth = login(tenant);
        }
        return cachedTenantAuth;
    }

    private CachedTenantAuth login(Tenant tenant) {
        String tenantAuthtype = tenant.getAuthtype();
        if ("basic".equals(tenantAuthtype)) {
            CachedTenantAuth cached = new CachedTenantAuth("Basic " + Base64.getEncoder()
                    .encodeToString((tenant.getUser() + ":" + tenant.getPassword()).getBytes()), null);
            cachedTenantAuths.put(tenant.getName(), cached);
            return cached;
        } else if ("oauth".equals(tenantAuthtype)) { // TODO implmenet oauth
            Exchange response = producerTemplate.send(e -> {
                e.getIn().setBody(tenant);
                e.setPattern(ExchangePattern.InOut);
            });
            return null;
        } else {
            throw new RuntimeException("Unsupported authType for local fsp: " + tenantAuthtype + ", for tenant: " + tenant.getName());
        }
    }

    private boolean isAccessTokenExpired(Date accessTokenExpiration) {
        if (accessTokenExpiration == null) // basic is stateless and has no expiration
            return false;

        Date fiveMinsFromNow = new Date(System.currentTimeMillis() + 300 * 1000);
        return accessTokenExpiration.before(fiveMinsFromNow);
    }
}