
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;

namespace Loader
{
    class Program
    {
        private static int[] Range;
        private static Random Rand = new Random(Environment.TickCount);

        static void Main(string[] args)
        {
            var digits = Enumerable.Range(48, 10);
            var uppercase = Enumerable.Range(65, 26);
            var lowercase = Enumerable.Range(97, 26);

            Range = digits.Concat(uppercase).Concat(lowercase).ToArray();

            var storage = CloudStorageAccount.Parse(ConfigurationManager.AppSettings["storage"]);

            var container = storage.CreateCloudBlobClient().GetContainerReference("input");

            var name = GetRandomText(10);

            var blob = container.GetBlockBlobReference(name);

            blob.Properties.ContentType = "text/plain";

            using (var writer = new StreamWriter(blob.OpenWrite()))
            {
                for (var i = 0; i < 10000; i++)
                {
                    writer.WriteLine(GetRandomText());
                }
            }

            var queue = storage.CreateCloudQueueClient().GetQueueReference("output");
            queue.CreateIfNotExists();

            queue = storage.CreateCloudQueueClient().GetQueueReference("input");
            queue.CreateIfNotExists();

            queue.AddMessage(new CloudQueueMessage("{ \"BlobName\": \"" + name + "\" }"));
        }

        static string GetRandomText(int length = 200)
        {
            var output = new char[length];

            for (var i = 0; i < length; i++)
            {
                output[i] = (char) Range[Rand.Next(0, Range.Length)];
            }

            return new string(output);
        }
    }
}
