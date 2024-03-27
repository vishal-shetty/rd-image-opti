package com.function.java;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.logging.Logger;
import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageOutputStream;
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
      InputStream optimizeImage = optimizeImage(imgFile, fileName, logger);
      long optimizedImageContentLength = getOptimizedImageContentLength(optimizeImage);
      logger.info("optimizedImageContentLength :: " + optimizedImageContentLength);

      // uploading file logic
      String thumbContainer = System.getenv("THUMB_CONTAINER");
      storeFile(fileName, optimizeImage, optimizedImageContentLength, contentType, imgConnectionStr, thumbContainer,
          logger);

    } catch (Exception e) {
      logger.severe("Error processing event: " + e.getMessage());
    }
  }

  public InputStream optimizeImage(InputStream imageInputStream, String fileName, Logger logger) throws IOException {
    // Read the image from the input stream
    BufferedImage image = ImageIO.read(imageInputStream);

    // get file extension
    String ext = getFileExtension(fileName);
    logger.info("file extension is :: " + ext);

    String compressionQuality = System.getenv("COMPRESSION_QUALITY"); // 0.75f

    // Get the image writer for the JPEG format
    Iterator<ImageWriter> writers = ImageIO.getImageWritersByFormatName(ext);
    ImageWriter writer = writers.next();

    // Create a new image output stream to store the optimized image
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ImageOutputStream ios = ImageIO.createImageOutputStream(baos);

    // Set the image write parameters to optimize the image quality
    ImageWriteParam writeParam = writer.getDefaultWriteParam();
    writeParam.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
    writeParam.setCompressionQuality(Float.valueOf(compressionQuality)); // Adjust the quality setting as needed

    // Write the optimized image to the output stream
    writer.setOutput(ios);
    writer.write(null, new IIOImage(image, null, null), writeParam);

    // Close the image writer and output stream
    writer.dispose();
    ios.close();

    logger.info("Image is optimized.");
    // Return the optimized image as an input stream
    return new ByteArrayInputStream(baos.toByteArray());
  }

  public long getOptimizedImageContentLength(InputStream imageStream) throws IOException {
    // Get the content length of the optimized image
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] buffer = new byte[1024];
    int bytesRead;
    while ((bytesRead = imageStream.read(buffer)) != -1) {
      baos.write(buffer, 0, bytesRead);
    }
    long contentLength = baos.size();

    // Close the output stream
    baos.close();

    // Return the content length
    return contentLength;
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
