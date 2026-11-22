using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Worker_cmd
{
    // Connection + messaging service class (module section 2.1.2, steps 3 and 5)
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

        public void WaitingMessage(string queue_name)
        {
            channel = connection.CreateModel();
            channel.QueueDeclare(queue: queue_name, durable: true, exclusive: false, autoDelete: false, arguments: null);

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                // RabbitMQ.Client 6.x delivers the body as ReadOnlyMemory<byte>, so convert it to an array first.
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                Console.WriteLine(" [x] Pesan diterima: {0}", message);

                // Each number in the message is one job: count down from that number.
                foreach (var part in message.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries))
                {
                    if (int.TryParse(part, out var start))
                    {
                        CountDown(start);
                    }
                    else
                    {
                        Console.WriteLine(" [!] '{0}' bukan angka, dilewati.", part);
                    }
                }
            };
            // autoAck = true, as in the module: a message is removed as soon as it is delivered.
            channel.BasicConsume(queue: queue_name, autoAck: true, consumer: consumer);

            Console.WriteLine(" Tekan [enter] untuk memutus koneksi.");
            Console.ReadLine();
            Disconnect();
        }

        // Module section 2.1.2 step 5: simulated job, one second per number.
        public void CountDown(int start)
        {
            for (int i = start; i > 0; i--)
            {
                Console.Write(i + " ");
                Thread.Sleep(1000);
            }
            Console.WriteLine("Done Computing");
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
        // Main function (module section 2.1.2, step 4: only the queue name is entered at runtime)
        static void Main(string[] args)
        {
            RMQ rmq = new RMQ();
            Console.WriteLine("Tekan tombol apapun untuk inisialisasi RMQ parameters.");
            Console.ReadKey();
            rmq.InitRMQConnection();
            Console.WriteLine("Tekan tombol apapun untuk membuka koneksi ke RMQ.");
            Console.ReadKey();
            rmq.CreateRMQConnection();
            Console.Write("Masukkan nama queue channel untuk menerima pesan melalui RMQ.\n>> ");
            string queue_name = Console.ReadLine();
            Console.WriteLine("Menunggu pesan masuk...");
            rmq.WaitingMessage(queue_name);
        }
    }
}
