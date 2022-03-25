namespace mqtt
{
    using System;
    using System.IO;
    using System.Net;
    using System.Runtime.InteropServices;
    using System.Runtime.Loader;
    using System.Security.Cryptography.X509Certificates;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Devices.Client;
    using Microsoft.Azure.Devices.Client.Transport.Mqtt;
    using MQTTnet;
    using MQTTnet.Client;
    using MQTTnet.Diagnostics;
    using MQTTnet.Formatter;
    using MQTTnet.Packets;

    class Program
    {
        static int counter;

        static void Main(string[] args)
        {
            Init().Wait();
            //
            // Wait until the app unloads or is cancelled
            var cts = new CancellationTokenSource();
            AssemblyLoadContext.Default.Unloading += (ctx) => cts.Cancel();
            Console.CancelKeyPress += (sender, cpe) => cts.Cancel();
            WhenCancelled(cts.Token).Wait();
        }

        /// <summary>
        /// Handles cleanup operations when app is cancelled or unloads
        /// </summary>
        public static Task WhenCancelled(CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<bool>();
            cancellationToken.Register(s => ((TaskCompletionSource<bool>)s).SetResult(true), tcs);
            return tcs.Task;
        }

        /// <summary>
        /// Initializes the ModuleClient and sets up the callback to receive
        /// </summary>
        static async Task Init()
        {
            MqttTransportSettings mqttSetting = new MqttTransportSettings(TransportType.Mqtt_Tcp_Only);
            ITransportSettings[] settings = { mqttSetting };
            // Open a connection to the Edge runtime
            ModuleClient ioTHubModuleClient = await ModuleClient.CreateFromEnvironmentAsync(settings);
            await ioTHubModuleClient.OpenAsync();
            Console.WriteLine("IoT Hub module client initialized.");

            //Subscribe MQTT Broker
            await MqttSubscribe(ioTHubModuleClient);

            // Register callback to be called when a message is received by the module
            //await ioTHubModuleClient.SetInputMessageHandlerAsync("input1", PipeMessage, ioTHubModuleClient);
        }
        static async Task  MqttSubscribe(object userContext)
        {
            var mqttFactory = new MqttFactory();
            MqttClient mqttClient= mqttFactory.CreateMqttClient();
            var mqttClientOptions = new MqttClientOptionsBuilder()
                .WithTcpServer("host.docker.internal", 1883)
                 .WithClientId(Dns.GetHostName())
                .WithKeepAlivePeriod(new TimeSpan(3, 23, 59, 59))
                .WithCleanSession()
                .WithWillTopic(Dns.GetHostName()+"WTopic")
                .WithProtocolVersion(MqttProtocolVersion.V500)
                .Build();

            mqttClient.ApplicationMessageReceivedAsync += e => {MessageReceivedAsync(e,userContext); return Task.CompletedTask; };
            
            await mqttClient.ConnectAsync(mqttClientOptions, CancellationToken.None);      

            var mqttSubscribeOptions = mqttFactory.CreateSubscribeOptionsBuilder()
            .WithTopicFilter("/bodymeasurement")
            .Build();
            await mqttClient.SubscribeAsync(mqttSubscribeOptions, CancellationToken.None);
        }
        static async void MessageReceivedAsync(MqttApplicationMessageReceivedEventArgs e, object userContext)
        {
            int counterValue = Interlocked.Increment(ref counter);
            string str = e.ApplicationMessage.ConvertPayloadToString();
            string strTopic = e.ApplicationMessage.Topic.ToString();
            var moduleClient = userContext as ModuleClient;
            if (moduleClient == null)
            {
                throw new InvalidOperationException("UserContext doesn't contain " + "expected values");
            }
            //var strOutput = System.Text.Json.JsonSerializer.Serialize(str); 
            Console.WriteLine($"Received message: {counterValue}, Body: [{str}]");
            var pipeMessage=new Message(Encoding.ASCII.GetBytes(str));
            pipeMessage.Properties.Add("messagetype", "normal");
            await moduleClient.SendEventAsync("output1", pipeMessage);
            Console.WriteLine("Received message sent");
            
        }//MessageReceivedAsync
    }
}
