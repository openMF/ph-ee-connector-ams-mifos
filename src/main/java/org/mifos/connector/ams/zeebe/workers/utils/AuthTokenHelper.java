package org.mifos.connector.ams.zeebe.workers.utils;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Base64.Encoder;

@Component
public class AuthTokenHelper {

    @Value("${fineract.auth-user}")
    private String authUser;

    @Value("${fineract.auth-password}")
    private String authPassword;

    private static final Encoder ENCODER = Base64.getEncoder();

    public String generateAuthToken() {
        byte[] base64 = ENCODER.encode((authUser + ":" + authPassword).getBytes());
        return "Basic " + new String(base64, StandardCharsets.ISO_8859_1);
    }
}
