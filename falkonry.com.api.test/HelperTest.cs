using System;
using System.IO;
using System.Linq;
using Xunit;
using Newtonsoft.Json;
using System.Collections.Generic;
using falkonry.com.util;

namespace falkonry.com.api.test
{
    public class HelperTest
    {
        static string rootPath = @"C:\Users\m2bre\Documents\Code\csharp\FlurSandbox\TestData";
        // Falkonry App2
        static string api = "https://app3.falkonry.ai/api/1.1";

        // Working Account 
        static string account = "596325951103639552";
        // mario.brenes@falkonry.com DataService Token
        static string token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE4Nzc5OTMxMTAsICJlbWFpbCIgOiAibWFyaW8uYnJlbmVzQGZhbGtvbnJ5LmNvbSIsICJuYW1lIiA6ICJtYXJpby5icmVuZXNAZmFsa29ucnkuY29tIiwgInNlc3Npb24iIDogIjU5Nzk1MTM0NTQyMzY1MDgxNiIgfQ.MZXzxIfKWqnNCPSiXS9ECLjlyYTxJQ2-3Wf0OK60lZ0";

        static string streamName = "F-MB Test API";

        static IList<string> filePaths = new List<string> {
            @"C:\Users\m2bre\Documents\Projects\Teekay\Data\ToBeProcessed\OAKSpirit\2017\09\K_2017_09_13.csv",
            @"C:\Users\m2bre\Documents\Projects\Teekay\Data\ToBeProcessed\OAKSpirit\2019\01\K_2019_01_09.csv",
            @"C:\Users\m2bre\Documents\Projects\Teekay\Data\ToBeProcessed\OAKSpirit\2019\02\K_2019_02_19.csv",
            @"C:\Users\m2bre\Documents\Projects\Teekay\Data\ToBeProcessed\TorbenSpirit\2018\11\K_2018_11_22.csv" };

        [Fact]
        public void CanceJobStreamNotExistTest()
        {
            // TODO: Make this not dependent of Aptar
            string acct = "637101632562577408";
            string tok = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE4ODc3MjYyNzYsICJlbWFpbCIgOiAibWFyaW8uYnJlbmVzQGZhbGtvbnJ5LmNvbSIsICJuYW1lIiA6ICJtYXJpby5icmVuZXNAZmFsa29ucnkuY29tIiwgInNlc3Npb24iIDogIjYzODc3NTIwNTA2Njg3ODk3NiIgfQ.x9fGscxaacWavrD5KvzrotzaQ0RE2QRA8jBZo_XLwEU";
            string job = "638738486124785664";
            var helper = new ApiHelper(acct, tok, api);
            Assert.Throws<AggregateException>(()=>helper.CancelJob(job));
            // var response = ;
            //Assert.NotNull(response);
            //Assert.True(response.status=="CANCELLED");
        }

        [Fact]
        public void CancelJobTest() 
        {
            // TODO: Make this not dependent of Aptar
            string acct = "637101632562577408";
            string tok = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE4ODc3MjYyNzYsICJlbWFpbCIgOiAibWFyaW8uYnJlbmVzQGZhbGtvbnJ5LmNvbSIsICJuYW1lIiA6ICJtYXJpby5icmVuZXNAZmFsa29ucnkuY29tIiwgInNlc3Npb24iIDogIjYzODc3NTIwNTA2Njg3ODk3NiIgfQ.x9fGscxaacWavrD5KvzrotzaQ0RE2QRA8jBZo_XLwEU";
            string job = "639020583443836928";
            var helper = new ApiHelper(acct, tok, api);
            var response = helper.CancelJob(job);
            Assert.NotNull(response);
            Assert.True(response.status == "CANCELLED");
        }

        [Fact]
        public void CancelStreamJobsTest()
        {
            // TODO: Make this not dependent of Aptar
            string acct = "637101632562577408";
            string tok = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE4ODc3MjYyNzYsICJlbWFpbCIgOiAibWFyaW8uYnJlbmVzQGZhbGtvbnJ5LmNvbSIsICJuYW1lIiA6ICJtYXJpby5icmVuZXNAZmFsa29ucnkuY29tIiwgInNlc3Npb24iIDogIjYzODc3NTIwNTA2Njg3ODk3NiIgfQ.x9fGscxaacWavrD5KvzrotzaQ0RE2QRA8jBZo_XLwEU";
            string stream = "637124151964635136";
            var helper = new ApiHelper(acct, tok, api);
            var response = helper.CancelStreamJobs(stream, new List<JOBTYPE> { JOBTYPE.INGESTDATA}, new List<JOBSTATUS> { JOBSTATUS.CREATED,JOBSTATUS.RUNNING });
            Assert.NotNull(response);
            Assert.True(response.Count > 0);
            foreach (var res in response)
            { 
                Assert.True(res.status == "CANCELLED");
            }
        }

        [Fact]
        public void NarrowFileTest()
        {
            var narrowStream = StreamFormat.CreateFromFile("narrow.json");
            Assert.NotNull(narrowStream);
            Assert.True(narrowStream is NarrowFormat);
        }

        [Fact]
        public void WideFileTest()
        {
            var wideStream = StreamFormat.CreateFromFile("wide.json");
            Assert.NotNull(wideStream);
            Assert.True(wideStream is StreamFormat);
        }

        [Fact]
        public void NarrowBatchFileTest()
        {
            var batchStream = StreamFormat.CreateFromFile("narrow_batch.json");
            Assert.NotNull(batchStream);
            Assert.True(batchStream is NarrowBatchFormat);
        }

        [Fact]
        public void BadFileTest()
        {
            Assert.Throws<ArgumentException>(()=>StreamFormat.CreateFromFile("missing_entity.json"));
        }

        [Fact]
        public void WideBatchFileTest()
        {
            var batchStream = StreamFormat.CreateFromFile("wide_batch.json");
            Assert.NotNull(batchStream);
            Assert.True(batchStream is BatchFormat);
        }


        [Fact]
        public void ConstructorTest()
        {
            ApiHelper helper = new ApiHelper(account, token, api);
            Assert.NotNull(helper);
        }

        [Fact]
        public void GetStreamsTest()
        {
            ApiHelper helper = new ApiHelper(account, token, api);
            var streams = helper.GetDataStreams();
            Assert.NotNull(streams);
            string sStreams = JsonConvert.SerializeObject(streams, Formatting.Indented);
            Console.WriteLine(sStreams);
        }

        [Fact]
        public void GetUsersTest()
        {
            ApiHelper helper = new ApiHelper(account, token, api);
            var users = helper.GetUsers();
            Assert.NotNull(users);
            string sUsers = JsonConvert.SerializeObject(users, Formatting.Indented);
            Console.WriteLine(sUsers);
        }

        [Fact]
        public void GetJobsTest()
        {
            ApiHelper helper = new ApiHelper(account, token, api);
            var jobs = helper.GetJobs();
            Assert.NotNull(jobs);
            string sJobs = JsonConvert.SerializeObject(jobs, Formatting.Indented);
            Console.WriteLine(sJobs);
        }

        [Fact]
        public void LoadCSVFileWideFormatTest()
        {
            ApiHelper helper = new ApiHelper(account, token, api);
            var files = from file in Directory.EnumerateFiles(Path.Combine(rootPath,"WideFormat"), "*Small1.csv", SearchOption.AllDirectories) select file;
            var apiHelper = new ApiHelper(account, token, api);
            var stream = apiHelper.SetDataStream(streamName, "GMT", MODELTYPE.SLIDING_WINDOW);
            // -d '{ "jobType": "INGESTDATA", "status": "CREATED", "datastream": "1554938538981549", "spec": { "format": {"entityIdentifier": "person", "timeIdentifier": "time", "timeFormat": "YYYY-MMDD HH:mm:ss.SSS", "timeZone": "America/Los_Angeles" } } }
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
                datastream = (string)stream.id,
                spec=spec
            };

            foreach (var file in files)
            {
                var response=apiHelper.LoadCSVFile(file, job);
                Assert.NotNull(response);
                Assert.Equal(response[response.Count-1].status,JOBSTATUS.COMPLETED.ToString());
            }
        }

        [Fact]
        public void LoadCSVFileWideFormatChunkedTest()
        {
            ApiHelper helper = new ApiHelper(account, token, api);
            var files = from file in Directory.EnumerateFiles(Path.Combine(rootPath, "WideFormat"), "*Medium5.csv", SearchOption.AllDirectories) select file;
            var apiHelper = new ApiHelper(account, token, api);
            var stream = apiHelper.SetDataStream(streamName, "GMT", MODELTYPE.SLIDING_WINDOW);
            // -d '{ "jobType": "INGESTDATA", "status": "CREATED", "datastream": "1554938538981549", "spec": { "format": {"entityIdentifier": "person", "timeIdentifier": "time", "timeFormat": "YYYY-MMDD HH:mm:ss.SSS", "timeZone": "America/Los_Angeles" } } }
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
                status = JOBSTATUS.CREATED,
                datastream = (string)stream.id,
                spec = spec
            };

            foreach (var file in files)
            {
                var response = apiHelper.LoadCSVFile(file, job,6U);
                Assert.NotNull(response);
                Assert.Equal(response[response.Count - 1].status, JOBSTATUS.COMPLETED.ToString());
            }
        }


        [Fact]
        public void LoadSmallCSVFilesWideFormatTest()
        {
            ApiHelper helper = new ApiHelper(account, token, api);
            var apiHelper = new ApiHelper(account, token, api);
            var dataStream = apiHelper.SetDataStream(streamName, "GMT", MODELTYPE.SLIDING_WINDOW);
            // -d '{ "jobType": "INGESTDATA", "status": "CREATED", "datastream": "1554938538981549", "spec": { "format": {"entityIdentifier": "person", "timeIdentifier": "time", "timeFormat": "YYYY-MMDD HH:mm:ss.SSS", "timeZone": "America/Los_Angeles" } } }
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
                status = JOBSTATUS.CREATED,
                datastream = dataStream,
                spec = spec
            };

            var response = apiHelper.LoadCSVFiles(filePaths, job, 3);
            Assert.NotNull(response);
            Assert.True(response.Count>(filePaths.Count+2));
        }
    }
}
