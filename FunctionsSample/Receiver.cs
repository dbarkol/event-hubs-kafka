using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace FunctionsSample
{
    public static class Receiver
    {
        // Retrieve the container name for the storage account
        private static string _containerName = Environment.GetEnvironmentVariable("ContainerName");

        [FunctionName("Receiver")]
        public static async Task Run(
            [EventHubTrigger("%EventHubName%", Connection = "EventHubConnectionString")] EventData[] events,
            [CosmosDB(
                databaseName: "%DatabaseName%",
                collectionName: "%CollectionName%",
                ConnectionStringSetting = "CosmosDBConnectionString")] IAsyncCollector<dynamic> documents, 
            ILogger log)
        {
            var exceptions = new List<Exception>();
            
            log.LogInformation("Receiver triggered.");

            // Create an empty byte array that will be 
            // filled with all the data from the events.
            byte[] data = new byte[0];

            foreach (var eventData in events)
            {
                try
                {
                    // Get the message body
                    var messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);

                    // Get the bytes of the message body and combine it with 
                    // the array that will be used update the file.
                    byte[] bytes = System.Text.Encoding.UTF8.GetBytes(messageBody);
                    data = Combine(data, bytes);

                    // Write the message to cosmos
                    await documents.AddAsync(new 
                    {
                        body = messageBody
                    });

                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be
                    // processed again later.
                    exceptions.Add(e);
                }
            }

            // If there is something in the byte array then append it to the 
            // file in storage. 
            if (data.Length > 0)
            {
                await AppendData(_containerName, "test.txt", data);
            }

            // Once processing of the batch is complete, if any messages in the batch
            // failed processing throw an exception so that there is a record of the failure.
            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }

        public static async Task AppendData(string containerName, string filename, byte[] data)
        {
            var storageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("StorageConnectionString"));
            var blobClient = storageAccount.CreateCloudBlobClient();

            // Create the container if it doesn't exist
            var container = blobClient.GetContainerReference(containerName);
            await container.CreateIfNotExistsAsync();

            // Create the blob if it doesn't exist
            var appendBlob = container.GetAppendBlobReference(filename);
            if (!await appendBlob.ExistsAsync())
            {
                await appendBlob.CreateOrReplaceAsync();
            }

            // Append 
            using (var stream = new System.IO.MemoryStream(data))
            {
                await appendBlob.AppendBlockAsync(stream);
            }
        }

        private static byte[] Combine(params byte[][] arrays)
        {
            byte[] rv = new byte[arrays.Sum(a => a.Length)];
            int offset = 0;
            foreach (byte[] array in arrays)
            {
                System.Buffer.BlockCopy(array, 0, rv, offset, array.Length);
                offset += array.Length;
            }
            return rv;
        }
    }
}
