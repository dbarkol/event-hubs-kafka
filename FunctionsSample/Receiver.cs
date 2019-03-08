using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Parquet;
using Parquet.Data;
using Encoding = System.Text.Encoding;

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

            // Define the schema for the parquet file
            var idColumn = new DataField<string>("id");
            var bodyColumn = new DataField<string>("body");

            // Retrieve a memory stream for the parquet information
            var ms = await GetParquetStream(_containerName, "Packet.parquet");
            ms.Position = 0;

            // Iterate through the events and append to the stream
            // the parquet row groups.
            foreach (var eventData in events)
            {             
               try
                {
                    // Get the message body
                    var messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);

                    // Check to see if this is going to be an append operation to the 
                    // parquet writer - it has to be explicitly set in the constructor.
                    var appendFlag = ms.Length > 0;

                    ms.Position = 0;                    
                    using (var writer = new ParquetWriter(new Schema(idColumn, bodyColumn), ms, append: appendFlag))
                    {
                        using (var rg = writer.CreateRowGroup())
                        {
                            rg.WriteColumn(new DataColumn(idColumn, new string[]
                            {
                                $"{Guid.NewGuid().ToString()}"
                            }));

                            rg.WriteColumn(new DataColumn(bodyColumn, new string[]
                            {
                                messageBody
                            }));
                        }
                    }
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be
                    // processed again later.
                    exceptions.Add(e);
                }
            }

            // Create/overwrite the existing file. 
            if (ms.Position > 0)
            {
                // Set the position to 0 before sending the stream to
                // create the file. 
                ms.Position = 0;
                await CreateParquetFile(_containerName, "Packet.parquet", ms);
            }

            // Once processing of the batch is complete, if any messages in the batch
            // failed processing throw an exception so that there is a record of the failure.
            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }

        private static async Task<MemoryStream> GetParquetStream(string containerName, string filename)
        {
            var storageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("StorageConnectionString"));
            var blobClient = storageAccount.CreateCloudBlobClient();

            // Create the container if it doesn't exist
            var container = blobClient.GetContainerReference(containerName);
            await container.CreateIfNotExistsAsync();

            // Return an empty memory stream if the blob does not exist
            var blob = container.GetBlockBlobReference(filename);
            if (!await blob.ExistsAsync())
            {
                return new MemoryStream();
            }

            // Download the existing blob
            var stream = new MemoryStream();
            await blob.DownloadToStreamAsync(stream);

            return stream;
        }

        private static async Task CreateParquetFile(string containerName, string filename, MemoryStream parquetStream)
        {
            var storageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("StorageConnectionString"));
            var blobClient = storageAccount.CreateCloudBlobClient();

            // Create the container if it doesn't exist
            var container = blobClient.GetContainerReference(containerName);
            await container.CreateIfNotExistsAsync();

            // Create the blob if it doesn't exist
            var blob = container.GetBlockBlobReference(filename);
            await blob.UploadFromStreamAsync(parquetStream);
        }

        private static async Task AppendData(string containerName, string filename, byte[] data)
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
