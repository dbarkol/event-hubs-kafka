using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

namespace FunctionsSample
{
    public static class Receiver
    {
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

            foreach (var eventData in events)
            {
                try
                {
                    // Get the message body
                    var messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);

                    // Send to cosmos
                    await documents.AddAsync(new 
                    {
                        body = messageBody
                    });

                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }
    }
}
