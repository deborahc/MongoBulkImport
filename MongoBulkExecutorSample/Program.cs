using Microsoft.Azure.CosmosDB.BulkExecutor;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MongoBulkExecutorSample
{
    using Microsoft.Azure.CosmosDB.BulkExecutor.BulkImport;
    using MongoDB.Bson;
    using System.Configuration;
    using System.Diagnostics;
    using System.Threading;

    class Program
    {

        private static readonly string EndpointUrl = ConfigurationManager.AppSettings["EndPointUrl"];
        private static readonly string AuthorizationKey = ConfigurationManager.AppSettings["AuthorizationKey"];
        private static readonly string DatabaseName = ConfigurationManager.AppSettings["DatabaseName"];
        private static readonly string CollectionName = ConfigurationManager.AppSettings["CollectionName"];


        public async static Task Main(string[] args)
        {
            new Program().Go().Wait();
        }


        public async Task Go()
        {
            while (true)
            {
                PrintPrompt();

                var c = Console.ReadKey(true);
                switch (c.Key)
                {
                    case ConsoleKey.D1:
                        await RunBulkScenario(CollectionName);
                        break;
                    case ConsoleKey.Escape:
                        Console.WriteLine("Exiting...");
                        return;
                    default:
                        Console.WriteLine("Invalid choice");
                        break;
                }
            }
        }

        private void PrintPrompt()
        {
            Console.WriteLine("Summary:");
            Console.WriteLine("--------------------------------------------------------------------- ");
            Console.WriteLine(String.Format("Endpoint: {0}", EndpointUrl));
            Console.WriteLine(String.Format("Collection : {0}.{1}", DatabaseName, CollectionName));
            Console.WriteLine("--------------------------------------------------------------------- ");
            Console.WriteLine("");
            Console.WriteLine("Press for bulk import scenario:\n");
            Console.WriteLine("1 - Bulk import data"); //TODO Move this out

            Console.WriteLine("--------------------------------------------------------------------- ");
        }

        private async Task RunBulkScenario(string collectionName)
        {

            var connectionPolicy = new ConnectionPolicy
            {
                ConnectionMode = ConnectionMode.Direct,
                ConnectionProtocol = Protocol.Tcp,
            };
            connectionPolicy.PreferredLocations.Add(LocationNames.WestUS2);
            MongoBulkExecutor mongoBulkExecutor = new MongoBulkExecutor(
                new Uri(EndpointUrl),
                AuthorizationKey,
                DatabaseName,
                collectionName,
                connectionPolicy
            );

            await mongoBulkExecutor.InitializeAsync();

            BulkImportResponse bulkImportResponse = null;
            long totalNumberOfDocumentsInserted = 0;
            double totalRequestUnitsConsumed = 0;
            double totalTimeTakenSec = 0;

            var tokenSource = new CancellationTokenSource();
            var token = tokenSource.Token;

            int numberOfBatches = int.Parse(ConfigurationManager.AppSettings["NumberOfBatches"]);
            long numberOfDocumentsPerBatch = long.Parse(ConfigurationManager.AppSettings["NumberOfDocumentsPerBatch"]);

            for (int i = 0; i < numberOfBatches; i++)
            {
                // Generate JSON-serialized documents to import.

                List<string> documentsToImportInBatch = new List<string>();
                long prefix = i * numberOfDocumentsPerBatch;

                Trace.TraceInformation(String.Format("Generating {0} documents to import for batch {1}", numberOfDocumentsPerBatch, i));

                documentsToImportInBatch = Util.GetSOHData(i, numberOfDocumentsPerBatch);


                // Invoke bulk import API.

                var tasks = new List<Task>();

                tasks.Add(Task.Run(async () =>
                {
                    Trace.TraceInformation(String.Format("Executing bulk import for batch {0}", i));
                    do
                    {
                        try
                        {
                            bulkImportResponse = await mongoBulkExecutor.BulkImportAsync(
                                documents: documentsToImportInBatch,
                                enableUpsert: false);
                        }
                        catch (DocumentClientException de)
                        {
                            Trace.TraceError("Document client exception: {0}", de);
                            break;
                        }
                        catch (Exception e)
                        {
                            Trace.TraceError("Exception: {0}", e);
                            break;
                        }
                    } while (bulkImportResponse.NumberOfDocumentsImported < documentsToImportInBatch.Count);

                    totalNumberOfDocumentsInserted += bulkImportResponse.NumberOfDocumentsImported;
                    totalRequestUnitsConsumed += bulkImportResponse.TotalRequestUnitsConsumed;
                    totalTimeTakenSec += bulkImportResponse.TotalTimeTaken.TotalSeconds;

                    // Code to summarize running total:
                    Console.WriteLine("--------------------------------------------------------------------- ");
                    Console.WriteLine(String.Format("Inserted {0} docs @ {1} writes/s, {2} RU/s in {3} sec",
                        totalNumberOfDocumentsInserted,
                        Math.Round(totalNumberOfDocumentsInserted / totalTimeTakenSec),
                        Math.Round(totalRequestUnitsConsumed / totalTimeTakenSec),
                        totalTimeTakenSec));
                    //Console.WriteLine(String.Format("Average RU consumption per document: {0}",
                    //    (totalRequestUnitsConsumed / totalNumberOfDocumentsInserted)));

                    //Console.WriteLine(String.Format("Total RU's consumed: {0}",
                    //    (totalRequestUnitsConsumed)));
                    //Console.WriteLine(String.Format("Total # of Documents inserted: {0}",
                    //    (totalNumberOfDocumentsInserted)));

                    //Trace.WriteLine(String.Format("\nSummary for batch {0}:", i));
                    //Trace.WriteLine("--------------------------------------------------------------------- ");
                    //Trace.WriteLine(String.Format("Inserted {0} docs @ {1} writes/s, {2} RU/s in {3} sec",
                    //    bulkImportResponse.NumberOfDocumentsImported,
                    //    Math.Round(bulkImportResponse.NumberOfDocumentsImported / bulkImportResponse.TotalTimeTaken.TotalSeconds),
                    //    Math.Round(bulkImportResponse.TotalRequestUnitsConsumed / bulkImportResponse.TotalTimeTaken.TotalSeconds),
                    //    bulkImportResponse.TotalTimeTaken.TotalSeconds));
                    //Trace.WriteLine(String.Format("Average RU consumption per document: {0}",
                    //    (bulkImportResponse.TotalRequestUnitsConsumed / bulkImportResponse.NumberOfDocumentsImported)));
                    //Trace.WriteLine("---------------------------------------------------------------------\n ");


                    Console.WriteLine("--------------------------------------------------------------------- ");

                },
                token));


                //tasks.Add(Task.Run(() =>
                //{
                //    char ch = Console.ReadKey(true).KeyChar;
                //    if (ch == 'c' || ch == 'C')
                //    {
                //        tokenSource.Cancel();
                //        Trace.WriteLine("\nTask cancellation requested.");
                //        Console.WriteLine("\nCancelling import.");

                //    }
                //}));


                await Task.WhenAll(tasks);

                Trace.WriteLine("\nPress any key to exit.");

            }

        }


        private static List<BsonDocument> GenerateSampleDocuments(int docsCount)
        {
            List<BsonDocument> sampleDocuments = new List<BsonDocument>();
            for (int i = 0; i < docsCount; i++)
            {
                BsonDocument dm = BsonSerializer.Deserialize<BsonDocument>("{\"guid\": \"16973980 - d66d - 4cdf - 8492 - 99c5496b2220\"}");
                dm.Set("guid", new BsonString(Guid.NewGuid().ToString()));
                sampleDocuments.Add(dm);
            }
            return sampleDocuments;
        }
    }

}
