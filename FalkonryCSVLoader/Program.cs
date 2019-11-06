/*
The MIT License
Copyright © 2010-2019 Falkonry.com
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

using System;
using System.IO;
using System.Linq;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Threading;
using Serilog;
using CommandLine;
using CommandLine.Text;
using Serilog.Events;

namespace falkonry.com.api
{
    class FalkonryCSVLoader
    {
        //static string rootPath = @"C:\Users\m2bre\Documents\Projects\Aptar\Data\Colmec\COLMEC_Data_Oct\Output\bi-vis 2";
        //static string rootPath = @"C:\Users\m2bre\Documents\Projects\BP\BP Kiwana data\Test";
        //static string rootPath = @"C:\Users\m2bre\Documents\Projects\XOM R&E\BMRF Coker 2018\SignalsOutput";

        // Falkonry App3
        //static string api = "https://app3.falkonry.ai/api/1.1";
        // Falkonry App2
        //static string api = "https://app2.falkonry.ai:30063/api/1.1";
        // Teekay LRS 2.0
        //static string api = "https://137.117.132.253/api/1.1";
        // App3 Falkonry Internal
        // App3 Aptar Injectables
        //static string account = "637101632562577408";
        //static string token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE4ODc3MjYyNzYsICJlbWFpbCIgOiAibWFyaW8uYnJlbmVzQGZhbGtvbnJ5LmNvbSIsICJuYW1lIiA6ICJtYXJpby5icmVuZXNAZmFsa29ucnkuY29tIiwgInNlc3Npb24iIDogIjYzODc3NTIwNTA2Njg3ODk3NiIgfQ.x9fGscxaacWavrD5KvzrotzaQ0RE2QRA8jBZo_XLwEU";
        // Teekay LRS 2.0
        //static string account = "Lsosgyy0b8cynn";
        //static string token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE4NzA5ODE4OTAsICJlbWFpbCIgOiAibWFyaW8uYnJlbmVzQGZhbGtvbnJ5LmNvbSIsICJuYW1lIiA6ICJtYXJpby5icmVuZXNAZmFsa29ucnkuY29tIiwgInNlc3Npb24iIDogIjE1NTU4OTUzMDY0NjY4NDUiIH0.s_cBIkUQbybLvMix9xCVC6lfkSeYrxMdFlFGmNI8MNQ";
        // Falkonry Internal
        //static string account = "596325951103639552";
        //static string token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE4Nzc5OTMxMTAsICJlbWFpbCIgOiAibWFyaW8uYnJlbmVzQGZhbGtvbnJ5LmNvbSIsICJuYW1lIiA6ICJtYXJpby5icmVuZXNAZmFsa29ucnkuY29tIiwgInNlc3Npb24iIDogIjU5Nzk1MTM0NTQyMzY1MDgxNiIgfQ.MZXzxIfKWqnNCPSiXS9ECLjlyYTxJQ2-3Wf0OK60lZ0";

        // static string streamName = @"Bi-vis 2";
        // static string streamName = "Prognost API Test";
        //static string streamName = "Aptar";
        // OR ID
        //static string datastream = "";

        //static string fileFilter = @"*.csv";
        //static string fileFilter = @"Htc_SKIP*.csv";
        //static string fileFilter = @"*filled.csv";
        //static string fileFilter = @"*cleansed.csv";

        static uint? DEFAULTBATCH = 100;
        static uint? DEFAULTSLEEP = 10;

        // Command Line Options
        class Options
        {
            [Usage(ApplicationAlias = "dotnet FalkonryCSVLoader.dll")]
            public static IEnumerable<Example> Examples
            {
                get
                {
                    return new List<Example>() {
                        new Example("Load CSV files to a Falkonry datastream (must specify either a stream name 'snam' or id 'sid'. Example invokation:", new Options { 
                            RootPath = @"~Documents/Project/CustomerX/Data",
                            ConfigPath=@"wide.json",
                            Uri=@"https://app3.falkonry.ai/api/1.1",
                            Account="1234567891011121314",
                            Token="t00L0NgToShoW1NOn3L1n3",
                            StreamName="CustomerX_ATypeMachines",

                        })
                    };
                }
            }
            [Option('r',"root", Required = true, HelpText = "Path to Root directory where files are located. Loader will traverse this directory and all descendant directories looking for files to load.  It will only load files that match the filter 'f' parameter.")]
            public string RootPath { get; set; }

            [Option('c',"conf", Required = true, HelpText = "Config File Path. Path to the stream configuration file (see examples: wide.json, narrow.json, etc.")]
            public string ConfigPath { get; set; }

            [Option('u', "uri", Required = true, HelpText = "Uri to Falkonry Service. Http endpoint used to invoke the root of the Falkonry API.  Example: https://app3.falkonry.ai/api/1.1")]
            public string Uri { get; set; }

            [Option('a', "acct", Required = true, HelpText = "Falkonry account id. Account id obtained from Url of Falkorny LRS UI.")]
            public string Account { get; set; }

            [Option('t', "tok", Required = true, HelpText = "Token id for accessing the API.")]
            public string Token { get; set; }

            [Option('j', "jsize", Required = false, HelpText = "Number of files per job.  This is a throttling parameter. A new job will be created every 'j' files.  Negative or Zero values will be ignored.  Default: 100.")]
            public uint? Batch { get; set; }

            [Option('s', "sleep", Required = false, HelpText = "Seconds to sleep after each job.  This is a throttling parameter to allow Falkonry to complete previous ingest jobs.  Negative values will be ignored. Default: 10")]
            public uint? Sleep { get; set; }

            [Option('f', "filt", Required = false, HelpText = "Files name filter.  Specify a filter in the form of a literal path plus the ? and * parameters.  Example:  '*cleaned*.csv'.  Default='*.csv'")]
            public string FilesFilter { get; set; }

            [Option('n', "snam", Required = false, HelpText = "Datastream Name. Specify datastream name.  If specified, it will be used to either create a new datastream or access and existing one.  This parameter is preferred over using 's' parameter.")]
            public string StreamName { get; set; }

            [Option('i', "sid", Required = false, HelpText = "Datastream id. Specify id of an existing datastream id obtained from Url of Falkorny LRS UI.  If this parameter is specified, the loader will not create a new stream and hence ignore parameter 'n'.")]
            public string StreamId{ get; set; }

        }
         
        static void printExceptionInformation(Exception e)
        {
            // Unwrapping
            if (e is System.AggregateException)
            {
                foreach (var ie in ((System.AggregateException)e).InnerExceptions)
                {
                    // Print inner exceptions
                    Console.WriteLine(ie.Message);
                    Console.WriteLine(ie.HelpLink);
                }
            }
            Console.WriteLine(e.Message);
            Console.WriteLine(e.HelpLink);
        }

        static void Main(string[] args)
        {
            // Create logger
            var logDir = $"{AppDomain.CurrentDomain.BaseDirectory}Log";
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .WriteTo.Console(restrictedToMinimumLevel: LogEventLevel.Information)
                .WriteTo.File(logDir + $"{Path.DirectorySeparatorChar}logfile.log", rollingInterval: RollingInterval.Day)
                .CreateLogger();

            // Arg Parsing 
            string rootPath = null;
            string configPath = null;
            string streamName = null;
            string datastream = null;
            string api = null;
            string account = null;
            string token = null;
            uint? batchSize = DEFAULTBATCH;
            uint? sleep = DEFAULTSLEEP;
            string fileFilter = "*.csv";
            Parser.Default.ParseArguments<Options>(args).WithParsed<Options>(o =>
            {
                rootPath = o.RootPath;
                configPath = o.ConfigPath;
                streamName = o.StreamName;
                datastream = o.StreamId;
                api = o.Uri;
                token = o.Token;
                account = o.Account;
                fileFilter = o.FilesFilter ?? fileFilter;
                sleep = o.Sleep ?? sleep;
                batchSize = o.Batch ?? batchSize;
            }).WithNotParsed<Options>((errs) => Environment.Exit(-1));

            // Clamp sleep and batch
            if (sleep < 0)
                sleep = DEFAULTSLEEP;
            if (batchSize <=0)
                batchSize = DEFAULTBATCH;

            // Validate that name of id specified
            if(String.IsNullOrEmpty(streamName) && String.IsNullOrEmpty(datastream))
                throw (new ArgumentException("Must specify a stream name or a stream id."));

            // This will be loaded from defined config file
            Log.Information($"Reading config file ");
            var sf = StreamFormat.CreateFromFile(configPath);
            var timeZone = sf.timeZone;
            var isBatchModel = (sf is BatchFormat || sf is NarrowBatchFormat);
                       
            Log.Information("Starting up CSV Loader");

            string jsonText = "";
            try
            {
                var acctProps = new Dictionary<string, string>(); 
                // acctProps["tercel.connector.flush.signal.batch.size"] = "25";


                var apiHelper = new ApiHelper(account, token, api, acctProps, responseCallback:
                            (response) => {
                                jsonText = JsonConvert.SerializeObject(response, Formatting.Indented,
                                    new JsonSerializerSettings
                                    {
                                        ReferenceLoopHandling = ReferenceLoopHandling.Ignore
                                        //ReferenceLoopHandling=ReferenceLoopHandling.Serialize,
                                        //PreserveReferencesHandling=PreserveReferencesHandling.Objects
                                    });
                                if (jsonText.Contains("excep"))
                                {
                                    Log.Error(jsonText);             
                                }
                                else
                                    Log.Debug(jsonText);
                            });

                // var exist = apiHelper.GetAccountProps(account);

                // Create datastream only if not exits
                if (String.IsNullOrEmpty(datastream))
                {
                    dynamic stream = apiHelper.SetDataStream($"{streamName}", timeZone, isBatchModel ? MODELTYPE.BATCH : MODELTYPE.SLIDING_WINDOW);
                    datastream = (string)stream.id;
                }

                // Get all csv files
                var info = new DirectoryInfo(rootPath);
                var filesInfo = info.GetFiles(fileFilter, SearchOption.AllDirectories);
                //var files = from file in Directory.EnumerateFiles(rootPath, "*cleansed.csv", SearchOption.AllDirectories) select file;
                var files = from file in filesInfo.Where(f=>f.Length>0) select file.FullName;
 //               files = from file in files where file.Contains("SignalsOutput") && !file.Contains("batches") select file;
                //                var files = from file in Directory.EnumerateFiles(rootPath, "*entity.csv", SearchOption.AllDirectories) select file;
                Console.WriteLine($"{files.Count().ToString()} files found.");
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
                    var loadResponse = apiHelper.LoadCSVFiles(files.ToList<string>(), job, (int)batchSize, (int)sleep);
                    jsonText=JsonConvert.SerializeObject(loadResponse, Formatting.Indented,
                        new JsonSerializerSettings
                        {
                            ReferenceLoopHandling = ReferenceLoopHandling.Ignore
                            //ReferenceLoopHandling=ReferenceLoopHandling.Serialize,
                            //PreserveReferencesHandling=PreserveReferencesHandling.Objects
                        });
                }
                catch (Exception e)
                {
                    // Print outer exception
                    printExceptionInformation(e);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception reading csv files from directory: {rootPath}");
                printExceptionInformation(ex);
            }

            Log.Debug("Shutting down CSV Loader");

            var dir = $"{AppDomain.CurrentDomain.BaseDirectory}Responses";
            if (!Directory.Exists(dir))
                Directory.CreateDirectory(dir);
            File.WriteAllText($"{dir}{Path.DirectorySeparatorChar}responses.txt", jsonText);
            return;

        }
    }
}
