using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Receiver
{
    // Connection + messaging service class (module section 2.2.2, steps 3 and 5)
    class RMQ
    {
        public ConnectionFactory connectionFactory;
        public IConnection connection;
        public IModel channel;

        public void InitRMQConnection(string host = "localhost", int port = 5672, string user = "guest", string pass = "guest")
        {
            connectionFactory = new ConnectionFactory();
            connectionFactory.HostName = host;
            connectionFactory.Port = port;
            connectionFactory.UserName = user;
            connectionFactory.Password = pass;
        }

        public void CreateRMQConnection()
        {
            connection = connectionFactory.CreateConnection();
            Console.WriteLine("Koneksi " + (connection.IsOpen ? "Berhasil!" : "Gagal!"));
        }

        public void WaitingMessage(string exchangeName = "logs")
        {
            channel = connection.CreateModel();
            channel.ExchangeDeclare(exchange: exchangeName, type: ExchangeType.Fanout, durable: true, autoDelete: false, arguments: null);

            // Anonymous queue: the broker picks a random name. It belongs to this connection only
            // and is deleted when the receiver disconnects. Every receiver gets its own copy of each message.
            var queueName = channel.QueueDeclare().QueueName;
            channel.QueueBind(queue: queueName, exchange: exchangeName, routingKey: "");
            Console.WriteLine(" [*] Waiting for '{0}'.", exchangeName);

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                // RabbitMQ.Client 6.x delivers the body as ReadOnlyMemory<byte>, so convert it to an array first.
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                Console.WriteLine(" [x] Pesan diterima: {0}", message);
            };
            channel.BasicConsume(queue: queueName, autoAck: true, consumer: consumer);

            Console.WriteLine(" Tekan [enter] untuk memutus koneksi.");
            Console.ReadLine();
            Disconnect();
        }

        public void Disconnect()
        {
            channel.Close();
            channel = null;
            Console.WriteLine("Channel ditutup!");
            if (connection.IsOpen)
            {
                connection.Close();
            }

            Console.WriteLine("Koneksi diputus!");
            connection.Dispose();
            connection = null;
        }
    }

    class Program
    {
        // Main function (module section 2.2.2, step 3: the exchange name is entered at runtime)
        static void Main(string[] args)
        {
            RMQ rmq = new RMQ();
            Console.WriteLine("Tekan tombol apapun untuk inisialisasi RMQ parameters.");
            Console.ReadKey();
            rmq.InitRMQConnection();
            Console.WriteLine("Tekan tombol apapun untuk membuka koneksi ke RMQ.");
            Console.ReadKey();
            rmq.CreateRMQConnection();
            Console.Write("Masukkan nama exchange untuk menerima pesan melalui RMQ (default: logs).\n>> ");
            var exchangeName = Console.ReadLine();
            if (string.IsNullOrWhiteSpace(exchangeName)) exchangeName = "logs";
            rmq.WaitingMessage(exchangeName);
        }
    }
}
