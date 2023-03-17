package org.mifos.connector.ams.model;

public class AccountNameResponseDTO {
    public String lei;
    public String image;
    public Name name;
    public class Name{
        public String firstName;
        public String lastName;
        public String nativeName;
        public String title;
    }
}
