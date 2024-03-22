package com.function.java;

import com.microsoft.azure.eventgrid.models.EventGridEvent;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.*;

public class ThumbnailFunction {

  @FunctionName("BlobTrigger")
  @StorageAccount("AzureWebJobsStorage")
    public void BlobTriggerToBlobTest(
        @EventGridTrigger(name = "event") EventGridEvent eventGridEvent,
        @BindingName("name") String fileName,
        @BlobInput(name = "input", dataType = "binary", path = "{data.url}") byte[] input,
        @BindingName("data.url") String blobUrl,
        final ExecutionContext context
    ) {
        context.getLogger().info("Java Blob trigger function BlobTriggerToBlobTest processed a blob.\n Name: " + fileName + "\n Size: " + input.length + " Bytes");
        //outputBlob.setValue(inputBlob);
    }
}
