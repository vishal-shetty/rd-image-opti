package com.function.java;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Iterator;
import java.util.logging.Logger;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriter;
import com.azure.core.http.rest.Response;
import com.azure.core.util.BinaryData;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
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
      InputStream imgFile = downloadFile(fileName, container, imgConnectionStr, logger);
      logger.info("file is downloaded here");

      // Logic for image optimization

      // get file extension
      String ext = getFileExtension(fileName);
      logger.info("file extension is :: " + ext);

      Iterator<ImageWriter> writers = ImageIO.getImageWritersByFormatName(ext);
      ImageWriter writer = writers.next();

      // uploading file
      String thumbContainer = System.getenv("THUMB_CONTAINER");
      storeFile(fileName, imgFile, Long.valueOf(contentLength), contentType, imgConnectionStr, thumbContainer, logger);

      logger.info("file is Uploaded in " + thumbContainer + " container :: " + ext);

    } catch (Exception e) {
      logger.severe("Error processing event: " + e.getMessage());
    }
  }

  private String getFileExtension(String fileName) {
    String extension = null;
    int i = fileName.lastIndexOf('.');
    if (i > 0) {
      extension = fileName.substring(i + 1);
    }
    return extension;
  }

  public InputStream downloadFile(String blobitem, String containerName, String connectionstring, Logger logger) {
    logger.info("creating connection");
    BlobContainerClient containerClient = containerClient(connectionstring, containerName, logger);
    BlobClient blobClient = containerClient.getBlobClient(blobitem);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    blobClient.downloadStream(os);
    InputStream is = new ByteArrayInputStream(os.toByteArray());
    logger.info("returning input stream");
    return is;
  }

  private BlobContainerClient containerClient(String connectionstring, String containerName, Logger logger) {
    logger.info("establishing connection for client");
    BlobServiceClient serviceClient = new BlobServiceClientBuilder().connectionString(connectionstring).buildClient();
    BlobContainerClient blobContainer = serviceClient.getBlobContainerClient(containerName);
    logger.info("got blob client here");
    return blobContainer;
  }

  public void storeFile(String filename, InputStream content, long length, String contentType,
      String connectionstring, String containerName, Logger logger) {
    logger.info("Azure store file BEGIN " + filename);
    BlobClient client = containerClient(connectionstring, containerName, logger).getBlobClient(filename);

    BlobHttpHeaders jsonHeaders = new BlobHttpHeaders().setContentType(contentType);
    BinaryData data = BinaryData.fromStream(content, length);
    BlobParallelUploadOptions options =
        new BlobParallelUploadOptions(data).setRequestConditions(new BlobRequestConditions()).setHeaders(jsonHeaders);
    Response<BlockBlobItem> uploadWithResponse = client.uploadWithResponse(options, null, Context.NONE);
    uploadWithResponse.getRequest().getUrl();

    logger.info("Azure store file END with status code" + uploadWithResponse.getStatusCode());
    if (uploadWithResponse.getStatusCode() != 201) {
      logger.severe("Error uploading file : " + uploadWithResponse);
    }
  }

}
