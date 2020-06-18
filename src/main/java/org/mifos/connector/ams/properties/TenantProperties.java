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

    public Tenant getTenant(String name) {
        return getTenants().stream()
                .filter(t -> t.getName().equals(name))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Tenant with name: " + name + ", not configuerd!"));
    }
}
