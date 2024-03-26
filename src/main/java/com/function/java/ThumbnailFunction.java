package com.function.java;

import com.microsoft.azure.eventgrid.models.EventGridEvent;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.*;

public class ThumbnailFunction {

  @FunctionName("BlobTrigger")
  //@StorageAccount("AzureWebJobsStorage")
    public void BlobTriggerToBlobTest(
        @BlobTrigger(name = "file",
               dataType = "binary",
               path = "northshirecc-qa-images/{name}",
               connection = "AzureWebJobsStorage") byte[] content,
  @BindingName("name") String filename,
        final ExecutionContext context
    ) {
        context.getLogger().info("Name: " + filename + " Size: " + content.length + " bytes");
    }

}
