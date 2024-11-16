package com.neu.flexquery.service;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

@Service
public class OauthService {
    public boolean verifier(String token) {
        try {
            String[] strings = token.split(" ");
            return verify(strings[1]);
        } catch (Exception e) {
            System.out.println("Validation failed: " + e);
            return false;
        }
    }

    protected ResponseEntity<String> getCall(String url) throws RestClientException {
        RestTemplate restTemplate = new RestTemplate();
        return restTemplate.getForEntity(url, String.class);
    }

    public boolean verify(String token) {
        try {
            String url = "https://oauth2.googleapis.com/tokeninfo?access_token=" + token;
            ResponseEntity<String> response = getCall(url);
            return response.getStatusCode() == HttpStatus.OK;
        } catch (RestClientException e) {
            System.out.println("Error while verifying token: " + e);
            return false;
        }
    }
}
