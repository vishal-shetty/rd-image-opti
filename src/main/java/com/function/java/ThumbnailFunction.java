package com.function.java;

import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
import com.azure.storage.blob.models.BlobProperties;
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
import io.github.techgnious.exception.VideoException;

public class ThumbnailFunction {

  @FunctionName("BlobTriggerFunction")
  public void run(@EventGridTrigger(name = "event") String event, final ExecutionContext context) {
    Logger logger = context.getLogger();
    final List<String> validImgExt = Arrays.asList("JPG", "JPEG", "PNG", "JFIF", "TIFF", "TIF", "BMP", "WEBP", "GIF");
    final List<String> validVdoExt = Arrays.asList("MP4", "MOV", "3GPP", "3GP");
    String originalFileName = null, fileContainerName = null, connectionString = null;
    boolean isSuccess = true;
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
        isSuccess = false;
        logger.info(eventName + " :: Event is not for blob created so can be ignored");
        return;
      }

      // Else check for putBlob event
      JsonNode dataNode = eventNode.get("data");
      String fileUrl = dataNode.get("url").asText();
      String contentType = dataNode.get("contentType").asText();
      String contentLength = dataNode.get("contentLength").asText();
      String fileName = fileUrl.substring(fileUrl.lastIndexOf('/') + 1, fileUrl.length());
      originalFileName = fileName;
      // logging details
      logger.info("Uploaded File URL :: " + fileUrl);
      logger.info("Uploaded File ContentType :: " + contentType);
      logger.info("Uploaded File contentLength :: " + contentLength);
      logger.info("Uploaded File FileName :: " + fileName);
      String connectionStr = System.getenv("AzureWebJobsStorage");
      connectionString = connectionStr;
      String ext = getFileExtension(fileName);
      logger.info("file extension :: " + ext);

      // now check here the extension of the file video/image
      if (validImgExt.contains(ext.toUpperCase())) {
        String suffix = getContainerSuffix(fileUrl, "-images/");
        String container = suffix + "-images"; // System.getenv("IMAGE_CONTAINER");
        fileContainerName = container;
        logger.info("processing images file here, containerName :: " + container);
        byte[] imgFile = downloadFile(fileName, container, connectionStr, logger);
        logger.info("image file is downloaded here");

        String oldExt = ext;
        List<String> extForConversion = Arrays.asList("PNG", "GFIF", "WEBP");
        if (extForConversion.contains(ext.toUpperCase())) {
          logger.info("converting image to jpg");
          imgFile = getConvertedImg(imgFile, "JPG");
          ext = "jpg";
          fileName = fileName.replace(oldExt, ext);
          logger.info("image is successfully converted to jpg");
          contentType = "image/jpg";
        }
        logger.info("contentType before uploading " + contentType);

        // Rotate the image based on the EXIF orientation
        int exifOrientation = getExifOrientation(imgFile);
        logger.info("file exifOrientation value :: " + exifOrientation);
        byte[] rotatedImage = rotateImage(imgFile, exifOrientation, ext);
        imgFile = rotatedImage;

        // Logic for image resize and compression
        byte[] optimizeImage = optimizeImage(imgFile, logger, 1920, 1080, ext);
        // uploading file logic
        String medContainer = suffix + "-medium"; // System.getenv("MEDIUM_CONTAINER");
        storeFile(fileName, optimizeImage, contentType, connectionStr, medContainer, logger);

        // logic for thumbnail only resize the image
        String thumbContainer = suffix + "-thumbnails"; // System.getenv("THUMB_CONTAINER");
        byte[] thumbImage = resizeImage(imgFile, 200, 200, ext);
        storeFile(fileName, thumbImage, contentType, connectionStr, thumbContainer, logger);
      } 
      else if (validVdoExt.contains(ext.toUpperCase())) {
        String suffix = getContainerSuffix(fileUrl, "-videos/");
        String container = suffix + "-videos"; // System.getenv("VIDEO_CONTAINER");
        fileContainerName = container;
        logger.info("processing video file here, containerName :: " + container);

        // download videoFile here and extract metadata as well
        BlobContainerClient containerClient = containerClient(connectionStr, container, logger);
        BlobClient blobClient = containerClient.getBlobClient(fileName);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        blobClient.downloadStream(os);
        BlobProperties properties = blobClient.getProperties();
        logger.info("file size " + properties.getBlobSize());
        Map<String, String> metadata = properties.getMetadata();
        for (String key : metadata.keySet()) {
          logger.info("key::" + key + " value::" + metadata.get(key));
        }
        byte[] videoFile = os.toByteArray();
        logger.info("video file is downloaded here");

        String vidContainer = suffix + "-videos-optimised"; // System.getenv("VIDEO_OPTIMISED_CONTAINER");
        if (metadata.get("width") == null || metadata.get("height") == null) {
          logger.info("can't process the file as metadata is not available.");
          isSuccess = false;
        } else {
          byte[] video720p = convertTo720p(videoFile, Integer.valueOf(metadata.get("width")),
              Integer.valueOf(metadata.get("height")), logger);
          int extStart = fileName.lastIndexOf('.');
          String storeFileId = fileName.substring(0, extStart);
          logger.info("got filename :: " + storeFileId);
          String ver1 = storeFileId + ".mp4";
          storeFile(ver1, video720p, "video/mp4", connectionStr, vidContainer, logger);
          // byte[] video1080p = convertTo1080p(videoFile);
          // String ver2 = storeFileId + "ver2.mp4";
          // storeFile(ver2, video1080p, "video/mp4", connectionStr, vidContainer, logger);
          logger.info("file upload is completed");
        }
      } else {
        logger.info("invalid file format.");
        isSuccess = false;
      }

    } catch (Exception e) {
      logger.severe("Error processing event: " + e.getMessage());
      isSuccess = false;
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      e.printStackTrace(pw);
      logger.severe(sw.toString());
    } finally {
      if (isSuccess) {
        // we need to delete the original file
        deleteBlobFile(connectionString, fileContainerName, originalFileName, logger);
        logger.info("delete the original file");
      } else {
        logger.info("there is exception so cant delete the file");
      }
    }
  }

  private String getContainerSuffix(String url, String fileType) {
    int index = url.indexOf(fileType);
    String suffix = url.substring(url.indexOf(".blob.core.windows.net/") + 23, index);
    return suffix;
  }

  private byte[] convertTo720p(byte[] videoFile, int ogWidth, int ogHeight, Logger logger)
      throws VideoException {
    IVCompressor compressor = new IVCompressor();
    IVSize customRes = new IVSize();
    int[] newDimesions = getNewDimesions(1280, 720, ogWidth, ogHeight);
    logger.info("original dimension " + ogWidth + "x" + ogHeight + " and new dimensions :: " + newDimesions[0] + "x"
        + newDimesions[1]);
    customRes.setWidth(newDimesions[0]);
    customRes.setHeight(newDimesions[1]);
    IVAudioAttributes audioAttribute = new IVAudioAttributes();
    // For good audio quality, a bit rate of 64 kbps to 128 kbps is common.
    audioAttribute.setBitRate(64000);
    // These settings depend on your original audio. For stereo audio, keep the channels at 2, and the
    // sampling rate at 44.1 kHz (typical for audio files).
    audioAttribute.setChannels(2);
    audioAttribute.setSamplingRate(44100);

    IVVideoAttributes videoAttribute = new IVVideoAttributes();
    // For 720p, a bit rate of around 2 Mbps to 4 Mbps is common
    videoAttribute.setBitRate(2000000); // 2Mbps

    // For 720p, a frame rate of 24 to 30 frames per second (fps) is typical.
    videoAttribute.setFrameRate(30); // 30fps
    videoAttribute.setSize(customRes);
    byte[] output = compressor.encodeVideoWithAttributes(videoFile, VideoFormats.MP4, audioAttribute, videoAttribute);
    return output;
  }

  public int[] getNewDimesions(int targetWidth, int targetHeight, int ogWidth, int ogHeight) {
    // check if original dimension is already in range
    if (ogWidth <= targetWidth && ogHeight <= targetHeight) {
      return new int[] {ogWidth, ogHeight};
    }

    // Calculate the aspect ratio
    double aspectRatio = (double) ogWidth / ogHeight;

    // Determine the new dimensions while maintaining aspect ratio
    int newWidth = targetWidth;
    int newHeight = (int) (targetWidth / aspectRatio);
    System.out.println("aspectRatio :: " + aspectRatio);

    if (newHeight > targetHeight) {
      newHeight = targetHeight;
      newWidth = (int) (targetHeight * aspectRatio);
    }
    // rounding off to nearest multiple of 10 for caculated new width and height
    int roundedWidth = (Math.round(newWidth / 10)) * 10;
    int roundedHeight = (Math.round(newHeight / 10)) * 10;
    return new int[] {roundedWidth, roundedHeight};
  }

  private byte[] convertTo1080p(byte[] videoFile) throws VideoException {
    IVCompressor compressor = new IVCompressor();
    IVSize customRes = new IVSize();
    customRes.setWidth(1920);
    customRes.setHeight(1080);
    IVAudioAttributes audioAttribute = new IVAudioAttributes();
    // For good audio quality, a bit rate of 64 kbps to 128 kbps is common.
    audioAttribute.setBitRate(64000);
    // These settings depend on your original audio. For stereo audio, keep the channels at 2, and the
    // sampling rate at 44.1 kHz (typical for audio files).
    audioAttribute.setChannels(2);
    audioAttribute.setSamplingRate(44100);

    IVVideoAttributes videoAttribute = new IVVideoAttributes();
    // For 1080p, a bit rate of around 4 Mbps to 8 Mbps is common.
    videoAttribute.setBitRate(6000000); // 6Mbps

    // For 720p, a frame rate of 24 to 30 frames per second (fps) is typical.
    videoAttribute.setFrameRate(24); // 30fps
    videoAttribute.setSize(customRes);
    byte[] output = compressor.encodeVideoWithAttributes(videoFile, VideoFormats.MP4, audioAttribute, videoAttribute);
    return output;
  }

  private void deleteBlobFile(String connectionString, String containerName, String blobName, Logger logger) {
    logger.info("deleting file " + blobName+ " from container :: "+containerName);
    BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().connectionString(connectionString).buildClient();
    BlobClient blobClient = blobServiceClient.getBlobContainerClient(containerName).getBlobClient(blobName);
    blobClient.delete();
    logger.info("Blob file deleted successfully!");
  }


  private byte[] getConvertedImg(byte[] imgFile, String format) throws IOException {
    BufferedImage image = ImageIO.read(new ByteArrayInputStream(imgFile));
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    ImageIO.write(image, format, outputStream);
    return outputStream.toByteArray();
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

  private static int getExifOrientation(byte[] image) {
    Metadata metadata;
    Integer index = 1;
    try {
      metadata = ImageMetadataReader.readMetadata(new ByteArrayInputStream(image));
      // Get the first directory with orientation information
      ExifIFD0Directory ifd0Directory = metadata.getFirstDirectoryOfType(ExifIFD0Directory.class);
      if (ifd0Directory != null) {
        index = ifd0Directory.getInteger(ExifIFD0Directory.TAG_ORIENTATION);
      }
    } catch (ImageProcessingException | IOException e) {
      index = 1;
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
