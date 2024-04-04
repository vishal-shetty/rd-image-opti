package com.function.java;

import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import org.imgscalr.Scalr;
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
import com.drew.imaging.ImageMetadataReader;
import com.drew.imaging.ImageProcessingException;
import com.drew.metadata.Metadata;
import com.drew.metadata.exif.ExifIFD0Directory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.EventGridTrigger;
import com.microsoft.azure.functions.annotation.FunctionName;
import io.github.techgnious.IVCompressor;
import io.github.techgnious.dto.IVAudioAttributes;
import io.github.techgnious.dto.IVSize;
import io.github.techgnious.dto.IVVideoAttributes;
import io.github.techgnious.dto.VideoFormats;
import java.io.PrintWriter;
import java.io.StringWriter;

public class ThumbnailFunction {

  @FunctionName("BlobTriggerFunction")
  public void run(@EventGridTrigger(name = "event") String event, final ExecutionContext context) {
    Logger logger = context.getLogger();
    final List<String> validImgExt = Arrays.asList("JPG", "JPEG", "PNG", "JFIF", "TIFF", "TIF", "BMP", "WEBP");
    final List<String> validVdoExt = Arrays.asList("MP4", "MOV", "3GPP", "3GP");
    try {

      ObjectMapper mapper = new ObjectMapper();
      logger.info("Blob uploaded: " + event);

      // Parse the event data
      JsonNode eventNode = mapper.readTree(event);

      JsonNode data = eventNode.get("data");
      String eventName = null;
      if (data != null) {
        eventName = data.get("api") != null ? data.get("api").asText() : null;
      }

      if (!"PutBlob".equals(eventName)) {
        logger.info(eventName + " :: Event is not for blob created so can be ignored");
        return;
      }

      // Else check for putBlob event
      JsonNode dataNode = eventNode.get("data");
      String fileUrl = dataNode.get("url").asText();
      String contentType = dataNode.get("contentType").asText();
      String contentLength = dataNode.get("contentLength").asText();
      String fileName = fileUrl.substring(fileUrl.lastIndexOf('/') + 1, fileUrl.length());

      // logging details
      logger.info("Uploaded File URL :: " + fileUrl);
      logger.info("Uploaded File ContentType :: " + contentType);
      logger.info("Uploaded File contentLength :: " + contentLength);
      logger.info("Uploaded File FileName :: " + fileName);
      String connectionStr = System.getenv("AzureWebJobsStorage");
      String ext = getFileExtension(fileName);
      logger.info("file extension :: " + ext);

      // now check here the extension of the file video/image
      if (validImgExt.contains(ext.toUpperCase())) {
        String container = System.getenv("IMAGE_CONTAINER");
        logger.info("processing images file here, containerName :: " + container);
        byte[] imgFile = downloadFile(fileName, container, connectionStr, logger);
        logger.info("image file is downloaded here");

        // Rotate the image based on the EXIF orientation
        int exifOrientation = getExifOrientation(imgFile);
        logger.info("file exifOrientation value :: " + exifOrientation);
        byte[] rotatedImage = rotateImage(imgFile, exifOrientation, ext);
        imgFile = rotatedImage;

        // Logic for image resize and compression
        byte[] optimizeImage = optimizeImage(imgFile, logger, 1920, 1080, ext);
        // uploading file logic
        String medContainer = System.getenv("MEDIUM_CONTAINER");
        storeFile(fileName, optimizeImage, contentType, connectionStr, medContainer, logger);

        // logic for thumbnail only resize the image
        String thumbContainer = System.getenv("THUMB_CONTAINER");
        byte[] thumbImage = resizeImage(imgFile, 200, 200, ext);
        storeFile(fileName, thumbImage, contentType, connectionStr, thumbContainer, logger);
      } 
      else if (validVdoExt.contains(ext.toUpperCase())) {
        String container = System.getenv("VIDEO_CONTAINER");
        logger.info("processing video file here, containerName :: " + container);
        byte[] videoFile = downloadFile(fileName, container, connectionStr, logger);
        logger.info("video file is downloaded here");

        IVCompressor compressor = new IVCompressor();
        IVSize customRes = new IVSize();
        customRes.setWidth(400);
        customRes.setHeight(300);
        IVAudioAttributes audioAttribute = new IVAudioAttributes();
        // here 64kbit/s is 64000
        audioAttribute.setBitRate(64000);
        audioAttribute.setChannels(2);
        audioAttribute.setSamplingRate(44100);

        IVVideoAttributes videoAttribute = new IVVideoAttributes();
        // Here 160 kbps video is 160000
        videoAttribute.setBitRate(160000);
        // More the frames more quality and size, but keep it low based on //devices like mobile
        videoAttribute.setFrameRate(15);
        videoAttribute.setSize(customRes);
        byte[] output =
            compressor.encodeVideoWithAttributes(videoFile, VideoFormats.MP4, audioAttribute, videoAttribute);
        String v1FileName = "v1_" + fileName;
        storeFile(v1FileName, output, contentType, connectionStr, container, logger);

      } else {
        logger.info("invalid file format.");
      }

    } catch (Exception e) {
      logger.severe("Error processing event: " + e.getMessage());
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      e.printStackTrace(pw);
      logger.severe(sw.toString());
    }

  }

  private static byte[] rotateImage(byte[] originalImage, int orientation, String ext) throws IOException {
    BufferedImage image = ImageIO.read(new ByteArrayInputStream(originalImage));
    double angle;
    switch (orientation) {
      case 3:
        angle = Math.PI; // 180 degrees
        break;
      case 6:
        angle = Math.PI / 2; // 90 degrees clockwise
        break;
      case 8:
        angle = -Math.PI / 2; // 90 degrees counterclockwise
        break;
      default:
        angle = 0; // No rotation
    }

    // Calculate the new dimensions after rotation
    int newWidth =
        (int) Math.abs(image.getWidth() * Math.cos(angle)) + (int) Math.abs(image.getHeight() * Math.sin(angle));
    int newHeight =
        (int) Math.abs(image.getWidth() * Math.sin(angle)) + (int) Math.abs(image.getHeight() * Math.cos(angle));

    BufferedImage rotatedImage = new BufferedImage(newWidth, newHeight, image.getType());
    Graphics2D g = rotatedImage.createGraphics();
    g.translate((newWidth - image.getWidth()) / 2, (newHeight - image.getHeight()) / 2);
    g.rotate(angle, image.getWidth() / 2.0, image.getHeight() / 2.0);
    g.drawImage(image, 0, 0, null);
    g.dispose();

    return imageToBytes(rotatedImage, ext);
  }

  private static byte[] imageToBytes(BufferedImage image, String ext) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ImageIO.write(image, ext, out);
    return out.toByteArray();
  }

  private static int getExifOrientation(byte[] image) throws IOException, ImageProcessingException {
    Metadata metadata = ImageMetadataReader.readMetadata(new ByteArrayInputStream(image));
    // Get the first directory with orientation information
    ExifIFD0Directory ifd0Directory = metadata.getFirstDirectoryOfType(ExifIFD0Directory.class);
    Integer index = 1;
    if (ifd0Directory != null) {
      index = ifd0Directory.getInteger(ExifIFD0Directory.TAG_ORIENTATION);
    }
    return index == null ? 1 : index;
  }

  public byte[] resizeImage(byte[] inputImage, int targetWidth, int targetHeight, String ext) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(inputImage);
    BufferedImage originalImage = ImageIO.read(in);

    // Resize the image using Imgscalr
    BufferedImage resizedImage = Scalr.resize(originalImage, Scalr.Method.QUALITY, targetWidth, targetHeight);

    // Convert the resized image back to a byte array
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ImageIO.write(resizedImage, ext, out);

    return out.toByteArray();
  }

  public byte[] optimizeImage(byte[] imageData, Logger logger, int targetWidth, int targetHeight, String ext)
      throws IOException {
    BufferedImage originalImage = ImageIO.read(new ByteArrayInputStream(imageData));
    int width = originalImage.getWidth();
    int height = originalImage.getHeight();
    if (width <= targetWidth && height <= targetHeight) {
      logger.info("returning from here no need to resize or compress the image");
      return imageData;
    }

    // Calculate the aspect ratio
    double aspectRatio = (double) width / height;

    // Determine the new dimensions while maintaining aspect ratio
    int newWidth = targetWidth;
    int newHeight = (int) (targetWidth / aspectRatio);

    if (newHeight > targetHeight) {
      newHeight = targetHeight;
      newWidth = (int) (targetHeight * aspectRatio);
    }

    logger.info("target width x height is " + targetWidth + "x" + targetHeight + " and Original Image aspect ratio :: "
        + width + "x" + height + " after calculating aspect ratio it is :: " + newWidth + "x" + newHeight);

    logger.info("need to resize and compress the image");
    Image scaledImage = scaleImage(newWidth, newHeight, originalImage);

    // Create an output stream for the compressed image
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    // Get the writer
    ImageWriter writer = ImageIO.getImageWritersByFormatName(ext).next();
    ImageWriteParam writeParam = writer.getDefaultWriteParam();
    writeParam.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
    writeParam.setCompressionQuality(0.8f); // 0.0f (max compression) to 1.0f (max quality)

    // Write the compressed image to the output stream
    writer.setOutput(ImageIO.createImageOutputStream(outputStream));
    writer.write(null, new IIOImage((BufferedImage) scaledImage, null, null), writeParam);
    writer.dispose();

    logger.info("resize and compress of the image is completed");
    return outputStream.toByteArray();
  }

  private String getFileExtension(String fileName) {
    String extension = null;
    int i = fileName.lastIndexOf('.');
    if (i > 0) {
      extension = fileName.substring(i + 1);
    }
    return extension;
  }

  public byte[] downloadFile(String blobitem, String containerName, String connectionstring, Logger logger) {
    logger.info("creating connection");
    BlobContainerClient containerClient = containerClient(connectionstring, containerName, logger);
    BlobClient blobClient = containerClient.getBlobClient(blobitem);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    blobClient.downloadStream(os);
    return os.toByteArray();
  }

  private BlobContainerClient containerClient(String connectionstring, String containerName, Logger logger) {
    logger.info("establishing connection for client");
    BlobServiceClient serviceClient = new BlobServiceClientBuilder().connectionString(connectionstring).buildClient();
    BlobContainerClient blobContainer = serviceClient.getBlobContainerClient(containerName);
    logger.info("got blob client here");
    return blobContainer;
  }

  public void storeFile(String filename, byte[] content, String contentType, String connectionstring,
      String containerName, Logger logger) {
    logger.info(
        "Azure store file BEGIN " + filename + " contentType ::" + contentType + " containerName :: " + containerName);
    BlobClient client = containerClient(connectionstring, containerName, logger).getBlobClient(filename);

    BlobHttpHeaders jsonHeaders = new BlobHttpHeaders().setContentType(contentType);
    BinaryData data = BinaryData.fromBytes(content);
    BlobParallelUploadOptions options =
        new BlobParallelUploadOptions(data).setRequestConditions(new BlobRequestConditions()).setHeaders(jsonHeaders);
    Response<BlockBlobItem> uploadWithResponse = client.uploadWithResponse(options, null, Context.NONE);
    uploadWithResponse.getRequest().getUrl();

    logger.info("Azure store file END with status code" + uploadWithResponse.getStatusCode());
    if (uploadWithResponse.getStatusCode() != 201) {
      logger.severe("Error uploading file : " + uploadWithResponse);
    }
  }

  private static BufferedImage scaleImage(int targetWidth, int targetHeight, BufferedImage originalImage) {
    int type = originalImage.getType() == 0 ? BufferedImage.TYPE_INT_ARGB : originalImage.getType();
    BufferedImage resizedImage = new BufferedImage(targetWidth, targetHeight, type);
    Graphics2D g = resizedImage.createGraphics();
    g.drawImage(originalImage, 0, 0, targetWidth, targetHeight, null);
    g.dispose();
    return resizedImage;
  }

}
