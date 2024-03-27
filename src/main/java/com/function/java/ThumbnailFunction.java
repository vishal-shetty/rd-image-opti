package com.function.java;

import java.io.ByteArrayOutputStream;
import java.util.logging.Logger;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
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
      
      // Parse the event data
      JsonNode eventNode = mapper.readTree(event);
      JsonNode dataNode = eventNode.get("data");
      String imageUrl = dataNode.get("url").asText();
      String contentType = dataNode.get("contentType").asText();
      String contentLength = dataNode.get("contentLength").asText();
      String fileName = imageUrl.substring(imageUrl.lastIndexOf('/') + 1, imageUrl.length());
      
      // logging details
      logger.info("Uploaded Image URL :: " + imageUrl);
      logger.info("Uploaded Image ContentType :: " + contentType);
      logger.info("Uploaded Image contentLength :: " + contentLength);
      logger.info("Uploaded Image fileName :: " + fileName);
      String imgConnectionStr = System.getenv("AzureWebJobsStorage");
      String container = System.getenv("IMAGE_CONTAINER");
      logger.info("images connection string :: " + imgConnectionStr + " container :: " + container);
      String downloadFile = downloadFile(fileName, container, imgConnectionStr, logger);
      logger.info("file is downloaded here :: " + downloadFile);

    } catch (Exception e) {
      logger.severe("Error processing event: " + e.getMessage());
    }
  }

  public String downloadFile(String blobitem, String containerName, String connectionstring, Logger logger) {
    logger.info("creating connection");
    BlobContainerClient containerClient = containerClient(connectionstring, containerName, logger);
    BlobClient blobClient = containerClient.getBlobClient(blobitem);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    blobClient.downloadStream(os);
    logger.info("returning os");
    return os.toString();
  }

  private BlobContainerClient containerClient(String connectionstring, String containerName, Logger logger) {
    logger.info("establishing connection for client");
    BlobServiceClient serviceClient = new BlobServiceClientBuilder().connectionString(connectionstring).buildClient();
    BlobContainerClient blobContainer = serviceClient.getBlobContainerClient(containerName);
    logger.info("got blob client here");
    return blobContainer;
  }
}
