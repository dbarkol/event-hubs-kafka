using System;
using System.IO;
using System.Threading.Tasks;
using Confluent.Kafka;
using FunctionsSample.Utils;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace FunctionsSample
{
    public static class Sender
    {
        private static Producer<long, string> KafkaProducer = null;
        private static string KafkaTopic = Environment.GetEnvironmentVariable("EventHubName");

        [FunctionName("Sender")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = null)] HttpRequest req,
            ExecutionContext context,
            ILogger log)
        {
            log.LogInformation("Sender invoked");

            // Retrieve the message body
            var requestBody = await new StreamReader(req.Body).ReadToEndAsync();

            // Deserialize it into a dynamic type
            dynamic data = JsonConvert.DeserializeObject(requestBody);

            // Retrieve an instance of the Kafka producer
            if (KafkaProducer == null) KafkaProducer = KafkaHelper.GetKafkaProducer(context, log);

            // Send to Kafka endpoint
            var deliveryReport = await KafkaProducer.ProduceAsync(KafkaTopic,
                DateTime.UtcNow.Ticks,
                requestBody);

            return (ActionResult)new OkObjectResult("");
        }
    }
}
