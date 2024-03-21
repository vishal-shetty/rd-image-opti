package com.function.java;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.OutputBinding;
import com.microsoft.azure.functions.annotation.*;

public class ThumbnailFunction {

  @FunctionName("BlobTrigger")
  @StorageAccount("AzureWebJobsStorage")
    public void BlobTriggerToBlobTest(
        @BlobTrigger(name = "triggerBlob", path = "test-triggerinput-java/{name}", dataType = "binary") byte[] triggerBlob,
        @BindingName("name") String fileName,
        @BlobInput(name = "inputBlob", path = "test-input-java/{name}", dataType = "binary") byte[] inputBlob,
        @BlobOutput(name = "outputBlob", path = "test-output-java/{name}", dataType = "binary") OutputBinding<byte[]> outputBlob,
        final ExecutionContext context
    ) {
        context.getLogger().info("Java Blob trigger function BlobTriggerToBlobTest processed a blob.\n Name: " + fileName + "\n Size: " + triggerBlob.length + " Bytes");
        outputBlob.setValue(inputBlob);
    }
}