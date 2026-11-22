using System.Text;
using RabbitMQ.Client;

namespace Broadcaster
{
    // Connection + messaging service class (module section 2.2.1, steps 2-8)
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

        public void CreateRMQChannel()
        {
            if (connection.IsOpen)
            {
                channel = connection.CreateModel();
                Console.WriteLine("Channel " + (channel.IsOpen ? "Berhasil!" : "Gagal!"));
            }
        }

        // Module step 8: the exchange name is a parameter, so it can be changed between A and B.
        public void SendMessage(string exchange, string msg = "send")
        {
            // The exchange is declared here (module step 2 declares it in CreateRMQChannel).
            // Declaring the same exchange again is safe, so the Broadcaster can switch between A and B.
            channel.ExchangeDeclare(exchange: exchange, type: ExchangeType.Fanout, durable: true, autoDelete: false, arguments: null);

            byte[] responseBytes = Encoding.UTF8.GetBytes(msg);

            // Module step 7: exchange = parameter, routingKey = "" (fanout ignores the routing key).
            channel.BasicPublish(exchange: exchange, routingKey: "", mandatory: false, basicProperties: null, body: responseBytes);
            Console.WriteLine("Pesan: '" + msg + "' telah dikirim ke exchange '" + exchange + "'.");
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
        // Main function (module section 2.2.1, step 9: Connect, then Kirim data, then Disconnect)
        static void Main(string[] args)
        {
            RMQ rmq = new RMQ();
            Console.WriteLine("Tekan tombol apapun untuk inisialisasi RMQ parameters.");
            Console.ReadKey();
            rmq.InitRMQConnection();
            Console.WriteLine("Tekan tombol apapun untuk membuka koneksi ke RMQ.");
            Console.ReadKey();
            rmq.CreateRMQConnection();
            rmq.CreateRMQChannel();

            Console.WriteLine("Isi tujuan broadcast (nama exchange) dan pesan. Ketik 'exit' pada pesan untuk keluar.");
            while (true)
            {
                Console.Write(">> tujuan broadcast (default: logs): ");
                var exchange = Console.ReadLine();
                if (string.IsNullOrWhiteSpace(exchange)) exchange = "logs";

                Console.Write(">> pesan: ");
                var inputMsg = Console.ReadLine();
                if (inputMsg == null || inputMsg == "exit") break;

                rmq.SendMessage(exchange, inputMsg);
                Thread.Sleep(200);
            }
            rmq.Disconnect();
        }
    }
}
