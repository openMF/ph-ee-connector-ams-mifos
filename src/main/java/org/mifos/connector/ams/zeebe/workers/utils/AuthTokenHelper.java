package org.mifos.connector.ams.zeebe.workers.utils;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Base64.Encoder;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class AuthTokenHelper {
	
	@Value("${fineract.auth-user}")
	private String authUser;
	
	@Value("${fineract.auth-password}")
	private String authPassword;

	private static final Encoder ENCODER = Base64.getEncoder();

	public String generateAuthToken() {
		String userPass = new StringBuilder(authUser).append(":").append(authPassword).toString();
		StringBuilder sb = new StringBuilder("Basic ");
		sb.append(new String(ENCODER.encode(userPass.getBytes()), StandardCharsets.ISO_8859_1));
		return sb.toString();
	}
}
