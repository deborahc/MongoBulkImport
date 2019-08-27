using Bogus;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MongoBulkExecutorSaple
{
    class Util
    {
        private static int PageSize = int.Parse(ConfigurationManager.AppSettings["NumberOfDocumentsPerBatch"]);

        static internal DocumentCollection GetCollectionIfExists(DocumentClient client, string databaseName, string collectionName)
        {
            return client.CreateDocumentCollectionQuery(UriFactory.CreateDatabaseUri(databaseName))
                .Where(c => c.Id == collectionName).AsEnumerable().FirstOrDefault();
        }

        internal static List<string> GetSOHData(int batchId, long numberOfDocumentsPerBatch)
        {
            StreamReader csvreader = new StreamReader(@"C:\Users\CosmosUser\Downloads\soh_batch_UAT.txt");
            //string inputLine = "";

            List<string> sohData = new List<string>();

            IEnumerable<string> page;
            List<string> lines = new List<string>();
            while ((page = NextPage(batchId)).Any()) // see which batch we're on
            {
                // but you can show it in the DataTable
                lines = page.ToList();

                for (var i = 0; i < lines.Count; i++)
                {
                    if (i==0 & batchId == 0)
                    {
                        continue;
                    }
                    var inputLine = lines[i];
                    var sohItem = new SOHItem();
                    string[] csvArray = inputLine.Split(new char[] { ',' });
                    sohItem.batchId = csvArray[0];
                    sohItem.Host_Store_No = csvArray[1];
                    sohItem.Ref_No = csvArray[2];
                    sohItem.UOM = csvArray[3];
                    sohItem.Stock_On_Hand = csvArray[4];
                    sohItem.LoadDateTimeUTC = DateTime.Parse(csvArray[5]);
                    sohItem.SOHDateTime = DateTime.Parse(csvArray[6]);
                    sohItem.CreateDateTimeUTC = DateTime.Parse(csvArray[7]);
                    sohItem.ProcessedFlag = csvArray[8];

                    sohItem.Z2Article = csvArray[9];
                    sohItem.FileDateSeconds = double.Parse(csvArray[10]);

                    sohData.Add(JsonConvert.SerializeObject(sohItem));
                }
                break;

                // Do processing

                //Console.Read();

            }

            return sohData;

        }
        private static IEnumerable<string> NextPage(int pageNumber)
        {
            var path = @"C:\Users\CosmosUser\Downloads\soh_batch_UAT.txt";

            IEnumerable<string> page = File.ReadLines(path)
                .Skip(pageNumber * PageSize).Take(PageSize);

            return page;
        }
    }

    class SOHItem
    {
        public string batchId { get; set; }
        public string Host_Store_No { get; set; }
        public string Ref_No { get; set; }
        public string UOM { get; set; }
        public string Stock_On_Hand { get; set; }
        public DateTime LoadDateTimeUTC { get; set; }
        public DateTime SOHDateTime { get; set; }
        public DateTime CreateDateTimeUTC { get; set; }
        public string Z2Article { get; set; }
        public string ProcessedFlag { get; set; }
        public double FileDateSeconds { get; set; }

    }
}