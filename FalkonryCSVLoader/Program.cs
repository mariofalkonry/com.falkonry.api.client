using System;
using System.IO;
using System.Linq;
using System.Text;
using Flurl;
using Flurl.Http;


namespace falkonry.com.api
{
    class FalkonryCSVLoader
    {
        static string rootPath = "C:\\Users\\m2bre\\Documents\\Projects\\Teekay\\Data\\ToBeProcessed";
        //static string rootPath = "C:\\Users\\m2bre\\Documents\\code\\python\\Falkonry\\Data\\2019";
        // Falkonry App2
        //static string api = "https://app2.falkonry.ai:30063/api/1.1";
        // Mario Brenes Working Account 
        //static string account = "lpwouijrq82p6o";
        //  Unknown Dataservice Token?
        //        static string token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE4NzA5ODgwNTUsICJlbWFpbCIgOiAibWFyaW8uYnJlbmVzQGZhbGtvbnJ5LmNvbSIsICJuYW1lIiA6ICJtYXJpby5icmVuZXNAZmFsa29ucnkuY29tIiwgInNlc3Npb24iIDogIjE1NTkwNTE0MDgxNjI5NDUiIH0.9Uzt0xeT4n5fhmQndwl5PjW69RiT5SAaB_6RZRuG2eE";
        // mario.brenes@falkonry.com DataService Token
        //static string token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE4NzA5ODkwOTYsICJlbWFpbCIgOiAibWFyaW8uYnJlbmVzQGZhbGtvbnJ5LmNvbSIsICJuYW1lIiA6ICJtYXJpby5icmVuZXNAZmFsa29ucnkuY29tIiwgInNlc3Npb24iIDogIjE1NTkwNTM0OTAzNjY1NjUiIH0._q5ZDvLlFi9blxBkk7dgcKaBej0zphLrxuBXIg4Kl0k";
        // namrata.rao@falkonry.com Dataservice Token?
        //static string token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE4NzA5ODg5NzYsICJlbWFpbCIgOiAibmFtcmF0YS5yYW9AZmFsa29ucnkuY29tIiwgIm5hbWUiIDogIm5hbXJhdGEucmFvQGZhbGtvbnJ5LmNvbSIsICJzZXNzaW9uIiA6ICIxNTU5MDUzMjUxMTY5MTAzIiB9.LFxVQrFxH13LC4duXjskKsV9OO_OSMzhfAG1PdJdfOE";
        // Burkhart Prognost at Falkonry
 //       static string datastream = "k8lrgpgwrtp9bl";
        // Teekay
        //static string api = "http://168.63.24.204:30063/api/1.1";
        static string api = "https://168.63.24.204/api/1.1"; // Skipping port for now
        static string account = "Lsosgyy0b8cynn";
        static string token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE4NzA5ODE4OTAsICJlbWFpbCIgOiAibWFyaW8uYnJlbmVzQGZhbGtvbnJ5LmNvbSIsICJuYW1lIiA6ICJtYXJpby5icmVuZXNAZmFsa29ucnkuY29tIiwgInNlc3Npb24iIDogIjE1NTU4OTUzMDY0NjY4NDUiIH0.s_cBIkUQbybLvMix9xCVC6lfkSeYrxMdFlFGmNI8MNQ";
        static string datastream= "lqbv47l2n8r26h";
        


        static void printExceptionInformation(Exception e)
        {
            Console.WriteLine($"Exception sending csv files to Falkonry uri: {api}");
            Console.WriteLine(e.Message);
            Console.WriteLine(e.HelpLink);
        }

        static void Main(string[] args)
        {
            // Get all csv files
            try
            {
                var files = from file in Directory.EnumerateFiles(rootPath, "*.csv", SearchOption.AllDirectories) select file;
                Console.WriteLine($"{files.Count().ToString()} files found.");
                var apiHelper = new ApiHelper(account, token, api);
                var sf = new StreamFormat()
                {
                    entityIdentifier = "entity",
                    timeIdentifier = "timestamp",
                    timeFormat = "YYYY-MM-DD HH:mm:ss",
                    timeZone = "Europe/London"
                };
                var spec = new IngestSpec<StreamFormat>()
                {
                    format = sf
                };
                var job = new Job<StreamFormat>()
                {
                    jobType = JOBTYPE.INGESTDATA,
                    status=JOBSTATUS.CREATED,
                    datastream = datastream,
                    spec = spec
                };

                try
                {
                    apiHelper.LoadCSVFiles(files.ToList<string>(),job,50);
                }
                catch (Exception e)
                {
                    // Unwrapping
                    if (e is System.AggregateException)
                    {
                        foreach (var ie in ((System.AggregateException)e).InnerExceptions)
                        {
                            // Print inner exceptions
                            printExceptionInformation(ie);
                        }
                    }
                    // Print outer exception
                    printExceptionInformation(e);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception reading csv files from directory: {rootPath}");
                Console.WriteLine(ex.Message);
            }
            return;
        }
    }
}
