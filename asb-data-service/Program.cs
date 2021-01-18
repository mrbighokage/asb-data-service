using System;
using System.Collections.Generic;
using System.Net.Mime;
using System.Text;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Newtonsoft.Json;

namespace asb_data_service
{
    static class Program
    {
        // TODO: will use from key-value secret storage
        private const string SERVICE_BUS_NAMESPACE_CONNECTION_STRING = "[...]";

        // TODO: will use from key-value secret storage
        private const string SERVICE_BUS_TOPIC_CONNECTION_STRING = "[...]";
        

        // TODO: will use from environment configuration
        private const string VEHICLE_EVENTS_TOPIC_NAME = "vehicle-events-topic";

        private static IMessageSender _messageSender;
        private static readonly Lazy<string> SessionId = new Lazy<string>(Guid.NewGuid().ToString());

        private static readonly List<ServiceBusSessionProcessor> ProcessorList = new List<ServiceBusSessionProcessor>();

        public static async Task Main(string[] args)
        {
            _messageSender = new MessageSender(SERVICE_BUS_TOPIC_CONNECTION_STRING, VEHICLE_EVENTS_TOPIC_NAME);

            // Send messages with sessionId set

            // Send event from Technicians
            _ = Task.Run(async () => await SendSessionMessagesAsync(35209308122263, "CAEAAAF0sKAIQAAB4GRnHmj0PgAXAEcVAEMAEgjvAfABFQQBAbMAAgADALQAB0I0KhgAQ83Ess4jokMQFgkAABMAXALxAABQeRAAZ5+GAU4AAAAAAAAAAAE="));

            // Send event from anybody else
            _ = Task.Run(async () => await SendSessionMessagesAsync(00000000000001, "Custom message body from another device"));


            // Receive Session based messages using SessionClient
            await using var serviceBusClient = new ServiceBusClient(SERVICE_BUS_NAMESPACE_CONNECTION_STRING);

            _ = Task.Run(async () => await ReceiveSessionMessagesAsync(serviceBusClient, "vehicle-events-development-subscription", async eventArgs =>
            {
                var body = eventArgs.Message.Body.ToString();
                Console.WriteLine($"============================ Received message from Development subscription ============================\n{body}\n");

                // complete the message. messages is deleted from the queue. 
                await eventArgs.CompleteMessageAsync(eventArgs.Message);
            }));

            _ = Task.Run(async () => await ReceiveSessionMessagesAsync(serviceBusClient, "vehicle-events-testing-subscription", async eventArgs =>
            {
                var body = eventArgs.Message.Body.ToString();
                Console.WriteLine($"============================ Received message from Testing subscription ============================\n{body}\n");

                // complete the message. messages is deleted from the queue. 
                await eventArgs.CompleteMessageAsync(eventArgs.Message);
            }));

            _ = Task.Run(async () => await ReceiveSessionMessagesAsync(serviceBusClient, "vehicle-events-staging-subscription", async eventArgs =>
            {
                var body = eventArgs.Message.Body.ToString();
                Console.WriteLine($"============================ Received message from Staging subscription ============================\n{body}\n");

                // complete the message. messages is deleted from the queue. 
                await eventArgs.CompleteMessageAsync(eventArgs.Message);
            }));

            _ = Task.Run(async () => await ReceiveSessionMessagesAsync(serviceBusClient, "vehicle-events-production-subscriptions", async eventArgs =>
            {
                var body = eventArgs.Message.Body.ToString();
                Console.WriteLine($"============================ Received message from Production subscription ============================\n{body}\n");

                // complete the message. messages is deleted from the queue. 
                await eventArgs.CompleteMessageAsync(eventArgs.Message);
            }));

            Console.ReadLine();

            Console.WriteLine("=========================================================");
            Console.WriteLine("Completed Receiving all messages... Press any key to exit");
            Console.WriteLine("=========================================================");

            // Stop processing for each topic subscriptions
            foreach (var processor in ProcessorList)
            {
                await processor.StopProcessingAsync();

                Console.WriteLine($"Stopped receiving messages {processor.EntityPath}");
            }

            await _messageSender.CloseAsync();
        }

        #region SendSessionMessages
        private static async Task SendSessionMessagesAsync(long imei, string body)
        {
            var messageObj = new
            {
                Body = body, // TODO: we can use it only as raw text in message body
                Imei = imei, // TODO: we can remove it because we have imei on custom UserProperties below
                DeviceTypeId = "teltonika", // TODO: we can move it to custom UserProperties
                ReceiveTimeUtc = DateTime.UtcNow // TODO: we can move it to custom UserProperties
            };

            var message = JsonConvert.SerializeObject(messageObj, Traxgo.AzureAppServiceConfiguration.Serializer.Json.Settings);
            // Create a new message to send to the queue
            var queueMessage = new Message(Encoding.UTF8.GetBytes(message))
            {
                SessionId = SessionId.Value,
                ContentType = MediaTypeNames.Application.Json,
                UserProperties = { { "imei", imei } }
            };
            await _messageSender.SendAsync(queueMessage);

            Console.WriteLine($"Sending message SessionId: {queueMessage.SessionId}; Topic {VEHICLE_EVENTS_TOPIC_NAME}; Imei: {imei} \n");
        }
        #endregion

        #region ReceiveSessionMessages

        private static async Task ReceiveSessionMessagesAsync(ServiceBusClient serviceBusClient, string subscriptionName, Func<ProcessSessionMessageEventArgs, Task> handler)
        {
            Console.WriteLine($"Subscribe for the \"{subscriptionName}\" \n");

            // create a processor that we can use to process the messages
            var processor = serviceBusClient.CreateSessionProcessor(VEHICLE_EVENTS_TOPIC_NAME, subscriptionName,
                new ServiceBusSessionProcessorOptions { SessionIds = { SessionId.Value } });

            // add handler to process messages
            processor.ProcessMessageAsync += handler;

            // add handler to process any errors
            processor.ProcessErrorAsync += ErrorHandler;

            // start processing 
            await processor.StartProcessingAsync();

            ProcessorList.Add(processor);
        }

        private static Task ErrorHandler(ProcessErrorEventArgs args)
        {
            Console.WriteLine(args.Exception.ToString());
            return Task.CompletedTask;
        }
        #endregion

    }
}