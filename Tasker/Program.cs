using System.Text;
using RabbitMQ.Client;

namespace Tasker
{
    // Connection + messaging service class (module section 2.1.1, steps 5-6)
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

        public void CreateRMQChannel(string queue_name)
        {
            if (connection.IsOpen)
            {
                channel = connection.CreateModel();
                Console.WriteLine("Channel " + (channel.IsOpen ? "Berhasil!" : "Gagal!"));

                // mandatory=true makes the broker return a message that no queue can receive.
                channel.BasicReturn += (sender, ea) =>
                {
                    var returned = Encoding.UTF8.GetString(ea.Body.ToArray());
                    Console.WriteLine($"[!] Pesan '{returned}' TIDAK sampai: tidak ada queue bernama '{ea.RoutingKey}' ({ea.ReplyText}).");
                };
            }

            if (channel.IsOpen)
            {
                channel.QueueDeclare(queue: queue_name, durable: true, exclusive: false, autoDelete: false, arguments: null);
                Console.WriteLine("Queue telah dideklarasikan..");
            }
        }

        public void SendMessage(string tujuan, string msg = "send")
        {
            byte[] responseBytes = Encoding.UTF8.GetBytes(msg);

            // Default exchange: the message goes to the queue named by "tujuan".
            channel.BasicPublish(exchange: "", routingKey: tujuan, mandatory: true, basicProperties: null, body: responseBytes);
            Console.WriteLine("Pesan: '" + msg + "' telah dikirim.");
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
        // Main function (module section 2.1.1: the Tasker UI is replaced by console input)
        static void Main(string[] args)
        {
            RMQ rmq = new RMQ();
            Console.WriteLine("Tekan tombol apapun untuk inisialisasi RMQ parameters.");
            Console.ReadKey();
            rmq.InitRMQConnection();
            Console.WriteLine("Tekan tombol apapun untuk membuka koneksi ke RMQ.");
            Console.ReadKey();
            rmq.CreateRMQConnection();
            rmq.CreateRMQChannel("workQ"); // same queue as PageLoadConnectRMQ in the module

            Console.WriteLine("Isi tujuan (nama queue worker) dan angka. Pisahkan angka dengan koma, contoh: 10,21,5,7,3");
            Console.WriteLine("Ketik 'exit' pada pesan untuk keluar.");
            while (true)
            {
                Console.Write(">> tujuan: ");
                var tujuan = Console.ReadLine() ?? "";
                Console.Write(">> pesan (angka): ");
                var inputMsg = Console.ReadLine();
                if (inputMsg == null || inputMsg == "exit") break;

                rmq.SendMessage(tujuan, inputMsg);
                Thread.Sleep(200); // give the broker time to return an unroutable message
            }
            rmq.Disconnect();
        }
    }
}
