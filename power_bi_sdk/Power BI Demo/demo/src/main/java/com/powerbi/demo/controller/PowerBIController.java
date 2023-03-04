package com.powerbi.demo.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.powerbi.demo.service.PowerBIAPIService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PowerBIController {

  @Autowired PowerBIAPIService powerBIAPIService;

  @PostMapping("/postDataset")
  public String createDataset(@RequestBody JsonNode dataset){
    return powerBIAPIService.postDataset(dataset);
  }

  @DeleteMapping("/deleteDataset")
  public String deleteDataset(@RequestParam String datasetId){
    return powerBIAPIService.deleteDataset(datasetId);
  }

  @GetMapping("/getDataset")
  public ResponseEntity<JsonNode> getDataset(@RequestParam String datasetId){
    return powerBIAPIService.getDataset(datasetId);
  }

  @PostMapping("/postRows")
  public String postRows(@RequestParam String tableName, @RequestParam String datasetId,@RequestBody JsonNode rows){
    return powerBIAPIService.postRows(datasetId,tableName,rows);
  }

  @GetMapping("/getAllRows")
  public ResponseEntity<JsonNode> getRows(@RequestParam String tableName, @RequestParam String datasetId){
    try {
      return powerBIAPIService.getRows(datasetId,tableName);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @PostMapping("/executeQuery")
  public ResponseEntity<JsonNode> executeDAXQuery(@RequestParam String datasetId,@RequestBody String query)
      throws JsonProcessingException {
    return powerBIAPIService.executeDAXQuery(datasetId,query);
  }

  @PostMapping("/updateRows")
  public String updateRow(@RequestParam String datasetId,@RequestParam String tableName,@RequestBody JsonNode insetedRow)
      throws JsonProcessingException {
    return powerBIAPIService.updateRows(datasetId,tableName,insetedRow);
  }

  @DeleteMapping("/deleteAllRows")
  public String deleteAllRows(@RequestParam String datasetId,@RequestParam String tableName){
    return powerBIAPIService.deleteAllRows(datasetId,tableName);
  }

  @DeleteMapping("/deleteSpecificRow")
  public String deleteSpecificRow(@RequestParam String datasetId,@RequestParam String tableName,@RequestBody JsonNode deletedNode)
      throws JsonProcessingException {
    return powerBIAPIService.deleteSpecificRow(datasetId,tableName,deletedNode);
  }

  @PutMapping("/replaceTable")
  public String replaceTable(@RequestParam String datasetId,@RequestParam String destinationTableName, @RequestParam String sourceTableName, @RequestBody JsonNode newTableSchema)
      throws JsonProcessingException {
    return powerBIAPIService.replaceTable(datasetId,destinationTableName,sourceTableName,newTableSchema);
  }

}
