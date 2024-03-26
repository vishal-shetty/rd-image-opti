package com.function.java;

import com.microsoft.azure.eventgrid.models.EventGridEvent;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.*;

public class ThumbnailFunction {

  	@FunctionName("BlobTriggerFunction")
    public void run(
        @EventGridTrigger(name = "event") String event,
        @BlobInput(
            name = "inputBlob",
            dataType = "binary",
            connection = "AzureWebJobsStorage",
            path = "northshirecc-qa-images/{name}"
        ) byte[] content,
        @BindingName("name") String blobName, // Dynamically extracted blob name
        final ExecutionContext context
    ) {
        try {
            // Parse the event data
            // You can use libraries like Jackson or Gson to deserialize the JSON event.
            // Extract container and blob names from the event data.
            context.getLogger().info("Blob uploaded: " + event);

            // Process the blob content (e.g., read, transform, etc.)
            // You can access the blob content via the 'content' parameter.
            // ...

            // You can also call other services, write to a database, etc.
            // ...
            context.getLogger().info("Blob name: " + blobName); // Dynamically extracted blob name

        } catch (Exception e) {
            context.getLogger().severe("Error processing event: " + e.getMessage());
        }
    } 

}
