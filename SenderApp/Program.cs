using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace SenderApp
{
    class Program
    {
        static void Main(string[] args)
        {
            var brokerList = "{event-hubs-fqdn:9093}"; 
            var connectionString = "{event-hubs-connection-string}";
            var topic = "{event-hub-name}";
            var caCertLocation = ".\\cacert.pem"; 
            var consumerGroup = "{consumer-group-name}"; 

            Console.WriteLine("Initializing Producer");
            Worker.Producer(brokerList, connectionString, topic, caCertLocation).Wait();
            Console.WriteLine();

            Console.ReadKey();
        }
    }

    class Worker
    {
        public static async Task Producer(string brokerList, string connStr, string topic, string cacertlocation)
        {
            try
            {
                var config = new Dictionary<string, object>
                {
                    {"bootstrap.servers", brokerList},
                    {"security.protocol", "SASL_SSL"},
                    {"sasl.mechanism", "PLAIN"},
                    {"sasl.username", "$ConnectionString"},
                    {"sasl.password", connStr},
                    {"ssl.ca.location", cacertlocation},
                    //{ "debug", "security,broker,protocol" }       //Uncomment for librdkafka debugging information
                };

                using (var producer =
                    new Producer<long, string>(config, new LongSerializer(), new StringSerializer(Encoding.UTF8)))
                {
                    Console.WriteLine("Sending 10 messages to topic: " + topic + ", broker(s): " + brokerList);
                    for (int x = 0; x < 10; x++)
                    {
                        var msg = string.Format("Sample message #{0} sent at {1}", x,
                            DateTime.Now.ToString("yyyy-MM-dd_HH:mm:ss.ffff"));
                        var deliveryReport = await producer.ProduceAsync(topic, DateTime.UtcNow.Ticks, msg);
                        Console.WriteLine(string.Format("Message {0} sent (value: '{1}')", x, msg));
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(string.Format("Exception Occurred - {0}", e.Message));
            }
        }
    }
}
