package org.mifos.connector.ams.properties;

public class Tenant {

    private String name;
    private String user;
    private String password;
    private String authtype;
    private String fspId;

    public Tenant() {}

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getAuthtype() {
        return authtype;
    }

    public void setAuthtype(String authtype) {
        this.authtype = authtype;
    }

    public String getFspId() {
        return fspId;
    }

    public void setFspId(String fspId) {
        this.fspId = fspId;
    }
}
