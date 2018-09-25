using Amqp;
using Amqp.Framing;
using System;
using System.Collections.Generic;
using System.Text;

namespace AmqpDemo
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.Write("Queue url: ");
            string queueUrl = Console.ReadLine();
            Console.Write("Receiver name: ");
            string receiverName = Console.ReadLine();
            Console.Write("Receiver address: ");
            string receiverAddress = Console.ReadLine();

            Connection connection = new Connection(new Address(queueUrl), null, null, OnConnectionOpened);
            Session session = new Session(connection);
            ReceiverLink receiverLink = new ReceiverLink(session, receiverName, receiverAddress);
            receiverLink.Start(10, OnMessage);
            connection.AddClosedCallback(OnConnectionClosed);

            Console.WriteLine("Press any key to exit...");
            Console.ReadKey(true);

            receiverLink.Close();
            session.Close();
            connection.Close();
        }

        private static void OnMessage(IReceiverLink link, Message message)
        {
            if (!(message.Body is string body))
            {
                if (message.Body is byte[] binary)
                    body = Encoding.UTF8.GetString(binary);
                else
                    throw new Exception($"Unexpected body type: {message.Body}");
            }

            Console.WriteLine(body);
            foreach (KeyValuePair<object, object> header in message.ApplicationProperties.Map)
                Console.WriteLine($"    {header.Key}: {header.Value}");

            if (message.ApplicationProperties.Map.ContainsKey("rollback")
                && message.ApplicationProperties["rollback"] is string rollback
                && rollback == "true")
            {
                link.Release(message);
                Console.WriteLine("Message released.");
            }
            else
            {
                link.Accept(message);
                Console.WriteLine("Message accepted.");
            }
        }

        private static void OnConnectionOpened(IConnection connection, Open open)
        {
            Console.WriteLine($"Connection opened: {open}");
        }

        private static void OnConnectionClosed(IAmqpObject sender, Error error)
        {
            Console.WriteLine($"Connection closed: {error}");
        }
    }
}
