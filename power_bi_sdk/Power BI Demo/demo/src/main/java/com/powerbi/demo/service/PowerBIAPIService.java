package com.powerbi.demo.service;

import static com.powerbi.demo.configuration.HttpHeaders.getHeaders;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class PowerBIAPIService {

  @Autowired RestTemplate restTemplate;
  String API_HOST="https://api.powerbi.com/v1.0/myorg/";

  public String postDataset(JsonNode dataset){
    String path=API_HOST+"/datasets";
    HttpEntity<JsonNode> httpEntity = new HttpEntity<>(dataset, getHeaders());
    ResponseEntity<String> result=restTemplate.exchange(path, HttpMethod.POST, httpEntity, String.class);

    return result.toString();
  }

  public String postRows(String datasetId, String tableName, JsonNode rows){
    String path=API_HOST+"/datasets/"+datasetId+"/tables/"+tableName+"/rows";

    HttpEntity<JsonNode> httpEntity = new HttpEntity<>(rows, getHeaders());
    restTemplate.exchange(path, HttpMethod.POST, httpEntity, String.class);

    return "Rows Inserted";
  }

  public ResponseEntity<JsonNode> getRows(String datasetId,String tableName)
      throws JsonProcessingException {
    String requestBody="{\n"
        + "  \"queries\": [\n"
        + "    {\n"
        + "      \"query\": \"EVALUATE VALUES("+tableName+")\"\n"
        + "    }\n"
        + "  ],\n"
        + "  \"serializerSettings\": {\n"
        + "    \"includeNulls\": true\n"
        + "  }\n"
        + "}";
    ObjectMapper mapper = new ObjectMapper();
    JsonNode newNode = mapper.readTree(requestBody);
    String path=API_HOST+"datasets/"+datasetId+"/executeQueries";
    HttpEntity<JsonNode> httpEntity = new HttpEntity<>(newNode, getHeaders());
    return restTemplate.exchange(path, HttpMethod.POST, httpEntity, JsonNode.class);
  }

  public ResponseEntity<JsonNode> executeDAXQuery(String datasetId, String query)
      throws JsonProcessingException {
    String requestBody="{\n"
        + "  \"queries\": [\n"
        + "    {\n"
        + "      \"query\": \""+query+"\""
        + "    }\n"
        + "  ],\n"
        + "  \"serializerSettings\": {\n"
        + "    \"includeNulls\": true\n"
        + "  }\n"
        + "}";
    ObjectMapper mapper = new ObjectMapper();
    JsonNode newNode = mapper.readTree(requestBody);
    String path=API_HOST+"datasets/"+datasetId+"/executeQueries";
    HttpEntity<JsonNode> httpEntity = new HttpEntity<>(newNode, getHeaders());
    return restTemplate.exchange(path, HttpMethod.POST, httpEntity, JsonNode.class);
  }

  public String updateRows(String datasetId, String tableName,JsonNode insetedRow)
      throws JsonProcessingException {

    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> result = mapper.convertValue(insetedRow, new TypeReference<Map<String, Object>>(){});

    String idKey = null;
    String idVal = null;
    for(Map.Entry m:result.entrySet()){
      String string=m.getKey().toString().toUpperCase();
      if(string.contains("ID")){
        idKey=m.getKey().toString();
        idVal=m.getValue().toString();
        break;
      }
    }
    JsonNode remainingVal=executeDAXQuery(datasetId,"EVALUATE FILTER("+tableName+","+tableName+"["+idKey+"]"+"<>"+idVal+")").getBody();
    JsonNode updatedJSON=getUpdatedJsonNodeForRowInsertion(tableName,remainingVal,insetedRow);
    deleteAllRows(datasetId,tableName);
    postRows(datasetId,tableName,updatedJSON);
    return "Row Updated";
  }

  public JsonNode getUpdatedJsonNodeForRowInsertion(String tableName,JsonNode filteredJSON, JsonNode insertedJSON)
      throws JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode filteredJSONRows = filteredJSON.get("results").get(0).get("tables").get(0).get("rows");
    String updatedVal;
    if(filteredJSONRows.toString().equals("[]")){
      updatedVal=insertedJSON.toString();
    }
    else{
      updatedVal=removeFirstandLast(filteredJSONRows.toString())+","+insertedJSON.toString();
    }
    updatedVal=updatedVal.replace(tableName+"[","");
    updatedVal=updatedVal.replace("]","");
    updatedVal="["+updatedVal+"]";
    filteredJSONRows=mapper.createObjectNode();
    ((ObjectNode) filteredJSONRows).put("rows", mapper.readTree(updatedVal));
    return filteredJSONRows;
  }

  public JsonNode getUpdatedJsonNodeForRowDeletion(String tableName,JsonNode filteredJSON)
      throws JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode filteredJSONRows = filteredJSON.get("results").get(0).get("tables").get(0).get("rows");
    String updatedVal=removeFirstandLast(filteredJSONRows.toString());
    updatedVal=updatedVal.replace(tableName+"[","");
    updatedVal=updatedVal.replace("]","");
    updatedVal="["+updatedVal+"]";
    filteredJSONRows=mapper.createObjectNode();
    ((ObjectNode) filteredJSONRows).put("rows", mapper.readTree(updatedVal));
    return filteredJSONRows;
  }

  public JsonNode getUpdatedJsonNodeForReplacement(String tableName,JsonNode oldTableRows)
      throws JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode filteredJSONRows = oldTableRows.get("results").get(0).get("tables").get(0).get("rows");
    String updatedVal=removeFirstandLast(filteredJSONRows.toString());
    updatedVal=updatedVal.replace(tableName+"[","");
    updatedVal=updatedVal.replace("]","");
    updatedVal="["+updatedVal+"]";
    filteredJSONRows=mapper.createObjectNode();
    ((ObjectNode) filteredJSONRows).put("rows", mapper.readTree(updatedVal));
    return filteredJSONRows;
  }

  public String deleteAllRows(String datasetId,String tableName){
    String path=API_HOST+"/datasets/"+datasetId+"/tables/"+tableName+"/rows";

    HttpEntity<JsonNode> httpEntity = new HttpEntity<>(null, getHeaders());
    restTemplate.exchange(path, HttpMethod.DELETE, httpEntity, String.class);

    return "All rows Deleted";
  }

  public String deleteSpecificRow(String datasetId,String tableName,JsonNode deletedNode)
      throws JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> result = mapper.convertValue(deletedNode, new TypeReference<Map<String, Object>>(){});

    String idKey = null;
    String idVal = null;
    for(Map.Entry m:result.entrySet()){
      String string=m.getKey().toString().toUpperCase();
      if(string.contains("ID")){
        idKey=m.getKey().toString();
        idVal=m.getValue().toString();
        break;
      }
    }
    JsonNode remainingRows=executeDAXQuery(datasetId,"EVALUATE FILTER("+tableName+","+tableName+"["+idKey+"]"+"<>"+idVal+")").getBody();
    JsonNode filteredJSONRows = getUpdatedJsonNodeForRowDeletion(tableName,remainingRows);
    deleteAllRows(datasetId,tableName);
    postRows(datasetId,tableName,filteredJSONRows);
    return "Specific row deleted";
  }

  public String replaceTable(String datasetId,String tableName, String sourceTable,JsonNode newTableSchema)
      throws JsonProcessingException {
    String path=API_HOST+"/datasets/"+datasetId+"/tables/"+tableName;
    HttpEntity<JsonNode> httpEntity = new HttpEntity<>(newTableSchema, getHeaders());
    restTemplate.exchange(path, HttpMethod.PUT, httpEntity, String.class);

    JsonNode oldTableRows=getRows(datasetId,sourceTable).getBody();
    oldTableRows=getUpdatedJsonNodeForReplacement(sourceTable,oldTableRows);
    deleteAllRows(datasetId,tableName);
    postRows(datasetId,tableName,oldTableRows);
    return "Table replacement successful";
  }

  public String deleteDataset(String datasetId){
    String path=API_HOST+"/datasets/"+datasetId;
    HttpEntity<JsonNode> httpEntity = new HttpEntity<>(null, getHeaders());
    restTemplate.exchange(path, HttpMethod.DELETE, httpEntity, String.class);
    return "Dataset Deleted";
  }

  public ResponseEntity<JsonNode> getDataset(String datasetId){
    String path=API_HOST+"/datasets/"+datasetId;
    HttpEntity<JsonNode> httpEntity = new HttpEntity<>(null, getHeaders());
    return restTemplate.exchange(path, HttpMethod.GET, httpEntity, JsonNode.class);
  }
  public static String removeFirstandLast(String str)
  {
    str = str.substring(1, str.length() - 1);
    return str;
  }
}
