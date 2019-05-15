using System;
using System.IO;
using System.Linq;
using Xunit;
using Newtonsoft.Json;
using System.Collections.Generic;

namespace falkonry.com.api.test
{
    public class HelperTest
    {
        //        static string rootPath = "C:\\Users\\m2bre\\Documents\\Projects\\Teekay\\Data\\ToBeProcessed";
        static string rootPath = "C:\\Users\\m2bre\\Documents\\code\\python\\Falkonry\\Data";
        // Falkonry App2
        static string api = "https://app2.falkonry.ai:30063/api/1.1";
        // Mario Brenes Working Account 
        static string account = "lpwouijrq82p6o";
        //  Unknown Dataservice Token?
        //        static string token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE4NzA5ODgwNTUsICJlbWFpbCIgOiAibWFyaW8uYnJlbmVzQGZhbGtvbnJ5LmNvbSIsICJuYW1lIiA6ICJtYXJpby5icmVuZXNAZmFsa29ucnkuY29tIiwgInNlc3Npb24iIDogIjE1NTkwNTE0MDgxNjI5NDUiIH0.9Uzt0xeT4n5fhmQndwl5PjW69RiT5SAaB_6RZRuG2eE";
        // mario.brenes@falkonry.com DataService Token
        static string token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE4NzA5ODkwOTYsICJlbWFpbCIgOiAibWFyaW8uYnJlbmVzQGZhbGtvbnJ5LmNvbSIsICJuYW1lIiA6ICJtYXJpby5icmVuZXNAZmFsa29ucnkuY29tIiwgInNlc3Npb24iIDogIjE1NTkwNTM0OTAzNjY1NjUiIH0._q5ZDvLlFi9blxBkk7dgcKaBej0zphLrxuBXIg4Kl0k";
        // namrata.rao@falkonry.com Dataservice Token?
        //static string token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE4NzA5ODg5NzYsICJlbWFpbCIgOiAibmFtcmF0YS5yYW9AZmFsa29ucnkuY29tIiwgIm5hbWUiIDogIm5hbXJhdGEucmFvQGZhbGtvbnJ5LmNvbSIsICJzZXNzaW9uIiA6ICIxNTU5MDUzMjUxMTY5MTAzIiB9.LFxVQrFxH13LC4duXjskKsV9OO_OSMzhfAG1PdJdfOE";
        // Burkhart Prognost at Falkonry
        static string datastream = "p76y6q7wprtdcv";
        // Teekay
        // static string api = "http://168.63.24.204:30063/api/1.1";
        //static string api = "https://168.63.24.204/api/1.1"; // Skipping port for now
        //static string account = "Lsosgyy0b8cynn";
        //static string token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE4NzA5ODE4OTAsICJlbWFpbCIgOiAibWFyaW8uYnJlbmVzQGZhbGtvbnJ5LmNvbSIsICJuYW1lIiA6ICJtYXJpby5icmVuZXNAZmFsa29ucnkuY29tIiwgInNlc3Npb24iIDogIjE1NTU4OTUzMDY0NjY4NDUiIH0.s_cBIkUQbybLvMix9xCVC6lfkSeYrxMdFlFGmNI8MNQ";
        // Burkhart Prognost at Falkonry
        //static string datastream = "9mqjdbcql7gvcv";

        static IList<string> filePaths = new List<string> {
            @"C:\Users\m2bre\Documents\Projects\Teekay\Data\ToBeProcessed\OAKSpirit\2017\09\K_2017_09_13.csv",
            @"C:\Users\m2bre\Documents\Projects\Teekay\Data\ToBeProcessed\OAKSpirit\2019\01\K_2019_01_09.csv",
            @"C:\Users\m2bre\Documents\Projects\Teekay\Data\ToBeProcessed\OAKSpirit\2019\02\K_2019_02_19.csv",
            @"C:\Users\m2bre\Documents\Projects\Teekay\Data\ToBeProcessed\TorbenSpirit\2018\11\K_2018_11_22.csv" };


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
        public void LoadCSVFilewWideFormatTest()
        {
            ApiHelper helper = new ApiHelper(account, token, api);
            var files = from file in Directory.EnumerateFiles(rootPath, "*.csv", SearchOption.AllDirectories) select file;
            var apiHelper = new ApiHelper(account, token, api);
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
                datastream = datastream,
                spec=spec
            };

            foreach (var file in files)
            {
                var response=apiHelper.LoadCSVFile(file, job);
                Assert.NotNull(response);
                Assert.Equal(response.status,JOBSTATUS.COMPLETED.ToString());
            }
        }

        [Fact]
        public void LoadCSVFileswWideFormatTest()
        {
            ApiHelper helper = new ApiHelper(account, token, api);
            var apiHelper = new ApiHelper(account, token, api);
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
                datastream = datastream,
                spec = spec
            };

            var response = apiHelper.LoadCSVFiles(filePaths, job, 3);
            Assert.NotNull(response);
            Assert.True(response.Count>(filePaths.Count+2));
        }
    }
}
