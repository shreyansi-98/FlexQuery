package com.neu.flexquery.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import com.neu.flexquery.config.RabbitMQConfig;
import com.neu.flexquery.service.OauthService;
import com.neu.flexquery.service.PlanService;
import com.neu.flexquery.util.JsonValidator;

import lombok.RequiredArgsConstructor;
import org.everit.json.schema.ValidationException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

@RestController
@RequestMapping("v1/plan")
@RequiredArgsConstructor
public class PlanController {

    @Autowired
    private JsonValidator jsonValidator;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private OauthService oauth;

    @Autowired
    private PlanService planService;

    @PostMapping(value = "/", produces = "application/json")
    public ResponseEntity<Object> createplan(@RequestHeader(value = "Authorization", required = false) String bearerToken,@RequestBody String planString) throws URISyntaxException {

      if (bearerToken == null) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(new JSONObject().put("error", "Please pass the Access Token").toString());
        }

        if (!oauth.verifier(bearerToken)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(new JSONObject().put("error", "Invalid Access Token").toString());
        }

        if (planString == null || planString.isEmpty()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new JSONObject().put("error", "empty request body").toString());
        }

        JSONObject planJson = new JSONObject(planString);
        try {
            jsonValidator.validateJson(planJson);
        } catch (ValidationException | IOException exception) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new JSONObject().put("error", exception.getMessage()).toString());
        }

        String planKey = planJson.get("objectType").toString() + "_" + planJson.get("objectId").toString();
        if (planService.checkIfKeyExists(planKey)) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(new JSONObject().put("error", "plan already exists!").toString());
        }

        String eTag = planService.saveplan(planKey, planJson.toString());

        //Push the message to the queue
        Map<String, String> message = new HashMap<>();
        message.put("operation", "SAVE");
        message.put("body", planString);
        rabbitTemplate.convertAndSend(RabbitMQConfig.queueName, message);

        return ResponseEntity.created(new URI("/plan/" + planJson.get("objectId").toString())).eTag(eTag).body(new JSONObject().put("message", "plan created successfully!")
                .put("planId", planJson.get("objectId").toString()).put("planKey", planKey).toString());

    }

    @GetMapping(value = "/{planId}", produces = "application/json")
    public ResponseEntity<Object> getplan(
            @RequestHeader(value = "Authorization", required = false) String bearerToken,
            @RequestHeader(value = HttpHeaders.IF_NONE_MATCH, required = false) String receivedETag,
            @PathVariable String planId) {

       if (bearerToken == null) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(new JSONObject().put("error", "Please pass the Access Token").toString());
        }

        if (!oauth.verifier(bearerToken)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(new JSONObject().put("error", "Invalid Access Token").toString());
        }

        String key = planId;
        if (!planService.checkIfKeyExists(key)) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(new JSONObject().put("error", "Plan not found").toString());
        }

        String oldETag = planService.getETag(key);
        if (receivedETag != null && receivedETag.equals(oldETag)) {
            return ResponseEntity.status(HttpStatus.NOT_MODIFIED).eTag(oldETag)
                    .body(new JSONObject().put("message", "plan not modified!").toString());
        }

        String plan = planService.getplan(key).toString();
        return ResponseEntity.ok().eTag(oldETag).body(plan);
    }

    @GetMapping(value = "/", produces = "application/json")
    public ResponseEntity<Object> getAllPlans(@RequestHeader(value = "Authorization", required = false) String bearerToken) {

        if (bearerToken == null) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(new JSONObject().put("error", "Please pass the Access Token").toString());
        }
        if (!oauth.verifier(bearerToken)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(new JSONObject().put("error", "Invalid Access Token").toString());
        }

        return ResponseEntity.ok().body(planService.getAllPlan().toString());
    }

    @DeleteMapping(value = "/{planId}", produces = "application/json")
    public ResponseEntity<Object> deleteplan(@PathVariable String planId,
                                             @RequestHeader(value = "Authorization", required = false) String bearerToken,
                                             @RequestHeader(value = HttpHeaders.IF_MATCH, required = false) String eTag) {

       if (bearerToken == null) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(new JSONObject().put("error", "Please pass the Access Token").toString());
        }

        if (!oauth.verifier(bearerToken)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(new JSONObject().put("error", "Invalid Access Token").toString());
        }

        String keyToDelete = planId;
        String oldETag = planService.getETag(keyToDelete);

        if (!planService.checkIfKeyExists(keyToDelete)) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(new JSONObject().put("error", "Plan  not found").toString());
        }

        if (eTag==null || !eTag.equals(oldETag)) {
            return ResponseEntity.status(HttpStatus.PRECONDITION_FAILED).body(new JSONObject().put("error", "the plan was modified!").toString());
        }

        //Push the message to the queue
        Map<String, String> message = new HashMap<>();
        message.put("operation", "DELETE");
        message.put("body", planService.getplan(keyToDelete).toString());
        rabbitTemplate.convertAndSend(RabbitMQConfig.queueName, message);

        if (planService.deleteplan(keyToDelete)) {
            return ResponseEntity.noContent().build();
        }

        return ResponseEntity.internalServerError().body(new JSONObject().put("error", "Internal server error").toString());
    }

    @PatchMapping(value = "/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> updatePlan(@RequestHeader(value = "Authorization", required = false) String bearerToken,
                                        @RequestHeader(value = HttpHeaders.IF_MATCH, required = false) String eTag,
                                        @RequestBody String requestBody, @PathVariable(value = "id") String id) throws Exception {

       if (bearerToken == null) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(new JSONObject().put("error", "Please pass the Access Token").toString());
        }

        if (!oauth.verifier(bearerToken)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(new JSONObject().put("error", "Invalid Access Token").toString());
        }

        if (eTag == null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new JSONObject().put("error", "No ETag Found").toString());
        }

        JSONObject newPlanObject = new JSONObject(requestBody);
        try {
            jsonValidator.validateJson(newPlanObject);
        } catch (ValidationException | IOException exception) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new JSONObject().put("error", exception.getMessage()).toString());
        }

        String oldPlan = planService.getplan(id).toString();
        String latestEtag = "";
        if(!StringUtils.hasText(oldPlan))
        {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(new JSONObject().put("error", "No Data Found").toString());
        }

        String oldETag = planService.getETag(id);

        if(!eTag.equals(oldETag))
        {
            return ResponseEntity.status(HttpStatus.PRECONDITION_FAILED).body(new JSONObject().put("error", "the plan was modified!").toString());
        }

        try
        {
            JSONObject oldPlanObject = new JSONObject(oldPlan);

            JSONArray planServicesNew = (JSONArray) newPlanObject.get("linkedplanServices");
            Set<JSONObject> planServicesSet = new HashSet<>();
            Set<String> objectIds = new HashSet<String>();
            planServicesNew.putAll((JSONArray) oldPlanObject.get("linkedplanServices"));
            for (Object object : planServicesNew) {
                JSONObject node = (JSONObject) object;
                String objectId = node.getString("objectId");
                        if (!objectIds.contains(objectId)) {
                            planServicesSet.add(node);
                            objectIds.add(objectId);
                        }
                    }
            planServicesNew.clear();
            if (!planServicesSet.isEmpty())
                planServicesSet.forEach(s -> {
                    planServicesNew.put(s);
                });
            latestEtag = planService.saveplan(id, newPlanObject.toString());

            //Push the message to the queue
            Map<String, String> message = new HashMap<>();
            message.put("operation", "SAVE");
            message.put("body", newPlanObject.toString());
            rabbitTemplate.convertAndSend(RabbitMQConfig.queueName, message);
        }
        catch(Exception e)
        {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new JSONObject().put("error", "Invalid Data!").toString());
        }

        return ResponseEntity.ok().eTag(latestEtag)
                .body(new JSONObject().put("message", "Patched data with key:" + id).toString());
    }
}
