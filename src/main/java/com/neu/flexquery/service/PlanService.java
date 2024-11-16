package com.neu.flexquery.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.neu.flexquery.dao.PlanDao;

import org.apache.commons.codec.digest.DigestUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Iterator;
import java.util.Map;

@Service
//@RequiredArgsConstructor
public class PlanService {
    private final PlanDao planDao;

    @Autowired
    private RedisTemplate redisTemplate;

    public PlanService(PlanDao planDao) {

        this.planDao = planDao;
    }
    public boolean checkIfKeyExists(String key) {
        return planDao.checkIfExists(key);
    }

    public String getETag(String key) {
        return planDao.hGet(key, "eTag");
    }

    public String saveplan(String key, String planJsonString) {
        String newETag = DigestUtils.md5Hex(planJsonString);

        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode rootNode = mapper.readTree(planJsonString);
            JsonNode planCostSharesNode = rootNode.get("planCostShares");

            String planCostSharesId = planCostSharesNode.get("objectType").textValue() + "_" + planCostSharesNode.get("objectId").textValue();
            //redisService.postValue(planCostSharesId, planCostSharesNode.toString());
            planDao.hSet(planCostSharesId, planCostSharesId, planCostSharesNode.toString());
            //redisTemplate.opsForValue().set(planCostSharesId, planCostSharesNode.toString());

            ArrayNode planServices = (ArrayNode) rootNode.get("linkedplanServices");
            for (JsonNode node : planServices) {
                Iterator<Map.Entry<String, JsonNode>> itr = node.fields();
                if (node != null) {
                    String linkedServicesId = (String) node.get("objectType").textValue() + "_" + node.get("objectId").textValue();
                    /*redisTemplate.opsForValue().set(linkedServicesId,
                            node.toString());*/
                    planDao.hSet(linkedServicesId, linkedServicesId, node.toString());
                }


                while (itr.hasNext()) {
                    Map.Entry<String, JsonNode> val = itr.next();
                    System.out.println(val.getKey());
                    System.out.println(val.getValue());
                    if (val.getKey().equals("linkedService")) {
                        JsonNode linkedServiceNode = (JsonNode) val.getValue();
                        String linkedServiceNodeId = (String) linkedServiceNode.get("objectType").textValue() + "_" + linkedServiceNode.get("objectId").textValue();
                        //redisTemplate.opsForValue().set(linkedServiceNodeId, linkedServiceNode.toString());
                        planDao.hSet(linkedServiceNodeId, linkedServiceNodeId, linkedServiceNode.toString());
                    }
                    if (val.getKey().equals("planserviceCostShares")) {
                        JsonNode planserviceCostSharesNode = (JsonNode) val.getValue();
                        String planserviceCostSharesId = (String) planserviceCostSharesNode.get("objectType").textValue() + "_" + planserviceCostSharesNode.get("objectId").textValue();
                        /*redisTemplate.opsForValue().set(
                                planserviceCostSharesId,
                                planserviceCostSharesNode.toString());*/
                        planDao.hSet(planserviceCostSharesId, planserviceCostSharesId, planserviceCostSharesNode.toString());
                    }
                }
            }

        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        planDao.hSet(key, key, planJsonString);
        planDao.hSet(key, "eTag", newETag);
        return newETag;
    }

    public JSONObject getplan(String key) {
        String planString = planDao.hGet(key, key);
        return new JSONObject(planString);
    }

    public JSONArray getAllPlan() {
        Map<String, String> planString = planDao.getAll();
        return new JSONArray(planString.values());
    }

    public boolean deleteplan(String key) {
        try {
            String planString = planDao.hGet(key, key);
            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(planString);
            JsonNode planCostSharesNode = rootNode.get("planCostShares");
            String planCostSharesId = planCostSharesNode.get("objectType").textValue() + "_" + planCostSharesNode.get("objectId").textValue();
            planDao.del(planCostSharesId);
            ArrayNode planServices = (ArrayNode) rootNode.get("linkedplanServices");
            for (JsonNode node : planServices) {
                Iterator<Map.Entry<String, JsonNode>> itr = node.fields();
                if (node != null)
                {
                    planDao.del(node.get("objectType").textValue() + "_" + node.get("objectId").textValue());
                }

                while (itr.hasNext()) {
                    Map.Entry<String, JsonNode> val = itr.next();
                    System.out.println(val.getKey());
                    System.out.println(val.getValue());
                    if (val.getKey().equals("linkedService")) {
                        JsonNode linkedServiceNode = (JsonNode) val.getValue();
                        planDao.del(linkedServiceNode.get("objectType").textValue() + "_"
                                + linkedServiceNode.get("objectId").textValue());

                    }
                    if (val.getKey().equals("planserviceCostShares")) {
                        JsonNode planserviceCostSharesNode = (JsonNode) val.getValue();
                        planDao.del(planserviceCostSharesNode.get("objectType").textValue() + "_"
                                + planserviceCostSharesNode.get("objectId").textValue());

                    }
                }
            }
        }
        catch (Exception e)
        {

        }
        return planDao.del(key) == 1;
    }

}
