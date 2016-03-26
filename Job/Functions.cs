using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Job
{
    public class Functions
    {
        //public static void ProcessBlobIncrementally([QueueTrigger("input")] BlobInfo info,
        //                                            [Blob("input/{BlobName}", FileAccess.Read)] Stream inputBlob,
        //                                            [Queue("output")] ICollector<BlobInfo> outputQueue,
        //                                            [Blob("output")] CloudBlobContainer outputContainer,
        //                                            TextWriter log)
        //{
        //    using (var reader = new StreamReader(inputBlob))
        //    {
        //        var pageCount = 1;
        //        var lines = new List<string>();

        //        while (!reader.EndOfStream)
        //        {
        //            var line = reader.ReadLine();

        //            line = new string(line.ToCharArray().Reverse().ToArray());

        //            lines.Add(line);

        //            if (lines.Count == 100)
        //            {
        //                var blobName = info.BlobName + ".page." + pageCount++;

        //                var outputBlob = outputContainer.GetBlockBlobReference(blobName);

        //                outputBlob.Properties.ContentType = "text/plain";

        //                using (var outStream = outputBlob.OpenWrite())
        //                using (var writer = new StreamWriter(outStream))
        //                {
        //                    lines.ForEach(writer.WriteLine);
        //                }

        //                outputQueue.Add(new BlobInfo { BlobName = blobName });

        //                lines.Clear();
        //            }
        //        }
        //    }
        //}

        public static void ProcessBlobAtTheEnd([QueueTrigger("input")] BlobInfo info,
                                               [Blob("input/{BlobName}", FileAccess.Read)] Stream inputBlob,
                                               [Queue("output")] ICollector<BlobInfo> outputQueue,
                                               [Blob("output")] CloudBlobContainer outputContainer,
                                               TextWriter log)
        {
            var tuples = new List<Tuple<string, List<string>>>();

            using (var reader = new StreamReader(inputBlob))
            {
                var pageCount = 1;
                var lines = new List<string>();

                while (!reader.EndOfStream)
                {
                    var line = reader.ReadLine();

                    line = new string(line.ToCharArray().Reverse().ToArray());

                    lines.Add(line);

                    if (lines.Count == 100)
                    {
                        var blobName = info.BlobName + ".page." + pageCount++;

                        tuples.Add(Tuple.Create(blobName, lines.ToList()));

                        lines.Clear();
                    }
                }
            }

            foreach (var tuple in tuples)
            {
                var blobName = tuple.Item1;
                var lines = tuple.Item2;

                var outputBlob = outputContainer.GetBlockBlobReference(blobName);

                outputBlob.Properties.ContentType = "text/plain";

                using (var outStream = outputBlob.OpenWrite())
                using (var writer = new StreamWriter(outStream))
                {
                    lines.ForEach(writer.WriteLine);
                }

                outputQueue.Add(new BlobInfo { BlobName = blobName });
            }
        }
    }
}
