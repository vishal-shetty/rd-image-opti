package com.function.java;

import java.util.logging.Logger;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.EventGridTrigger;
import com.microsoft.azure.functions.annotation.FunctionName;

public class ThumbnailFunction {
  
  @FunctionName("BlobTriggerFunction")
  public void run(@EventGridTrigger(name = "event") String event, final ExecutionContext context) {
    Logger logger = context.getLogger();
    try {

      ObjectMapper mapper = new ObjectMapper();
      logger.info("Blob uploaded: " + event);
      logger.info("Hello world Java...");
      
      // Parse the event data
      JsonNode eventNode = mapper.readTree(event);
      JsonNode dataNode = eventNode.get("data");
      String imageUrl = dataNode.get("url").asText();
      String contentType = dataNode.get("contentType").asText();
      String contentLength = dataNode.get("contentLength").asText();
      
      // logging details
      logger.info("Uploaded Image URL :: " + imageUrl);
      logger.info("Uploaded Image ContentType :: " + contentType);
      logger.info("Uploaded Image contentLength :: " + contentLength);

    } catch (Exception e) {
      logger.severe("Error processing event: " + e.getMessage());
    }
  }
}

