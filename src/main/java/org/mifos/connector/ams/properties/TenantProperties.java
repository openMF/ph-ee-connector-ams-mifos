package org.mifos.connector.ams.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@ConfigurationProperties(prefix = "ams.local")
public class TenantProperties {

    private List<Tenant> tenants = new ArrayList<>();

    public TenantProperties() {
    }

    public List<Tenant> getTenants() {
        return tenants;
    }

    public void setTenants(List<Tenant> tenants) {
        this.tenants = tenants;
    }

    public Tenant getTenant(String tenant) {
        return getTenants().stream()
                .filter(t -> t.getName().equals(tenant))
                .findFirst()
                .orElse(null);
    }

    public Tenant getTenant(String partyIdType, String partyId) {
        if (partyIdType == null || partyId == null) {
            return null;
        }

        return getTenants().stream()
                .filter(t -> t.getPartyIdType().equals(partyIdType) && t.getPartyId().equals(partyId))
                .findFirst()
                .orElse(null);
    }
}
