using System;
using System.Collections.Generic;
using Flurl;
using Flurl.Http;
using System.Net.Http;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using System.Text;
using System.Net.Http.Headers;
using Newtonsoft.Json.Converters;
using System.IO;
using System.Dynamic;
// TODO: Logging

namespace falkonry.com.api
{
    public enum JOBTYPE
    {
        NONE,INGESTDATA // More to be added in the future
    }

    public enum JOBSTATUS
    {
        NONE,CREATED,COMPLETED
    }

    public interface IApiHelper
    {
        dynamic LoadCSVFile<T>(string filepath,Job<T> job) where T:StreamFormat;

        IList<dynamic> LoadCSVFiles<T>(IList<string> filepaths, Job<T> job,int blocksize=20) where T : StreamFormat;

        IList<dynamic> GetDataStreams();
        IList<dynamic> GetJobs();
        IList<dynamic> GetUsers();
    }

    //WIDE - {"jobType": "INGESTDATA", "status": "CREATED", "datastream": "1554938538981549", "spec": { "format": {"entityIdentifier": "person", "timeIdentifier": "time", "timeFormat": "YYYY-MMDD HH:mm:ss.SSS", "timeZone": "America/Los_Angeles" } } }
    //NARROW - {"jobType": "INGESTDATA", "status": "CREATED", "datastream": "1554943865611097", "spec": { "format": { "signalIdentifier": "signal", "entityIdentifier": "entity", "valueIdentifier": "value", "timeIdentifier": "time", "timeFormat": "micros", "timeZone": "America/Los_Angeles" } } }' 
    public class StreamFormat
    {
        public string entityIdentifier { get; set; }
        public string timeIdentifier { get; set; }
        public string timeFormat { get; set; }
        public string timeZone { get; set; }
    }

    public class BatchFormat:StreamFormat
    {
        public string batchIdentifier { get; set; }
    }

    public class NarrowStreamFormat:StreamFormat
    {
        public string valueIdentifier { get; set; }
        public string signalIdentifier { get; set; }
    }

    public class NarrowBatchStreamFormat:NarrowStreamFormat
    {
        public string batchIdentifier { get; set; }
    }

    public class IngestSpec<T> where T:StreamFormat
    {
        public T format { get; set; }
    }

    public class Job<T> where T : StreamFormat
    {
        [JsonConverter(typeof(StringEnumConverter))]
        public JOBTYPE jobType { get; set; }

        [JsonConverter(typeof(StringEnumConverter))]
        public JOBSTATUS status { get; set; }

        public string datastream { get; set; }
        public IngestSpec<T> spec { get; set; }
    }

    public class ApiHelper : IApiHelper
    {
        static int MINFILEBLOCK = 2;
        static int MAXSTREAMS = 20;
        static int MAXASSESSMENTS = 10;
        static int MAXMODELS = 30;
        static int MAXJOBS = 30;
        static int MAXUSERS = 20;
        string _account = "";
        string _endpointUri = "https://localhost:30063/api/1.1";
        string _token = "";
        string _baseUri = "";

        public ApiHelper(string account, string token, string endpointUri = "https://localhost:30063/api/1.1")
        {
            if (string.IsNullOrEmpty(token))
                throw (new ArgumentNullException(nameof(token)));
            this._token = token;
            if (string.IsNullOrEmpty(account))
                throw (new ArgumentNullException(nameof(account)));
            this._account = account;
            if (string.IsNullOrEmpty(endpointUri))
                Console.WriteLine($"No api endpoint uri passed using {_endpointUri}");
            else if (IsValidUri(endpointUri))
                _endpointUri = endpointUri;
            else
                throw (new ArgumentException($"Uri {endpointUri} is not valid"));
            _baseUri = _endpointUri.Substring(0, endpointUri.IndexOf("/api/"));
            // TODO: remove this later
            // Configure to ignore bad certificates for this endpoint
            FlurlHttp.ConfigureClient(_endpointUri, cli =>
                cli.Settings.HttpClientFactory = new UntrustedCertClientFactory());
        }

        private bool IsValidUri(string uri)
        {
            try
            {
                Uri testUri = new Uri(uri);
            }
            catch (UriFormatException e)
            {
                return false;
            }
            return true;
        }

        #region IApiHelper interface
        public IList<dynamic> GetDataStreams()
        {
            // Get list of data streams            
            var streams = _endpointUri
            .AppendPathSegments("accounts", _account, "datastreams")
            .SetQueryParams(new { limit = MAXSTREAMS })
            .WithOAuthBearerToken(_token)
            .GetJsonListAsync().Result;

            // For each datastream get assessments
            foreach (var stream in streams)
            {
                // Get list of assessments
                string streamId = stream.id;
                stream.assessments = _endpointUri.AppendPathSegments("accounts", _account, "datastream", streamId, "assessments")
                    .SetQueryParams(new { limit = MAXASSESSMENTS })
                    .WithOAuthBearerToken(_token)
                    .GetJsonListAsync().Result;

                // For each assessment get models
                foreach (var assessment in stream.assessments)
                {
                    // Get list of models
                    string assessmentId = assessment.id;
                    assessment.models = _endpointUri.AppendPathSegments("accounts", _account, "datastream", streamId, "assessment", assessmentId, "models")
                        .SetQueryParams(new { limit = MAXMODELS })
                        .WithOAuthBearerToken(_token)
                        .GetJsonListAsync().Result;
                }
            }
            return streams;
        }

        //curl -X POST --header 'Content-type: text/csv' --header 'Authorization: Bearer [token]' --data-binary "@/Users/User1/Desktop/HumanActivity/source1.csv" 'http://<serveraddress>:30063/api/1.1/accounts/1549746082718454/datastreams/1554938538981549/ingestdata/1554942688319300/inputs'
        public dynamic LoadCSVFile<T>(string filepath, Job<T> jobObj) where T : StreamFormat
        {
            // Create Job
            //var jobInJson = JsonConvert.SerializeObject(jobObj);
            dynamic jobResponse;
            // Send data
            //curl -X POST --header 'Content-type: text/csv' --header 'Authorization: Bearer [token]' --data-binary "@/Users/User1/Desktop/HumanActivity/source1.csv" 'http://<serveraddress>:30063/api/1.1/accounts/1549746082718454/datastreams/1554938538981549/ingestdata/1554942688319300/inputs
            var fileContent = new StringContent(File.ReadAllText(filepath));
            //            var fileContent = new ByteArrayContent(File.ReadAllBytes(filepath));
            try
            {
                jobResponse = _endpointUri
                .AppendPathSegments("accounts", _account, "jobs")
                .WithOAuthBearerToken(_token)
                .PostJsonAsync(jobObj)
                .ReceiveJson().Result;
                // var jsonResponse = JsonConvert.SerializeObject(jobResponse);

                var fileResponse = new Url(_baseUri + jobResponse.links[0].url)
                .WithHeader("Content-type", "text/csv")
                .WithOAuthBearerToken(_token)
                .PostAsync(fileContent)
                .ReceiveJson().Result;

                // Complete file ingest job
                var complJobObj = new Job<T>();

                complJobObj.jobType = JOBTYPE.INGESTDATA;
                complJobObj.status = JOBSTATUS.COMPLETED;
                complJobObj.datastream = jobObj.datastream;
                complJobObj.spec = jobObj.spec;
                jobResponse = _endpointUri
                .AppendPathSegments("accounts", _account, "jobs", (string)jobResponse.id)
                .WithOAuthBearerToken(_token)
                .PutJsonAsync(complJobObj)
                .ReceiveJson().Result;
            }
            catch (Exception e)
            {
                // TODO logging
                // Unwrapping
                if (e is System.AggregateException)
                {
                    foreach (var ie in ((System.AggregateException)e).InnerExceptions)
                    {
                        if (ie is FlurlHttpException)
                        {
                            string msg = ((FlurlHttpException)ie).GetResponseStringAsync().Result;
                            ie.HelpLink = msg;
                        }
                    }
                }

                throw (e);
            }

            return jobResponse;
        }

        public IList<dynamic> LoadCSVFiles<T>(IList<string> filepaths, Job<T> jobObj, int blocksize = 10) where T : StreamFormat
        {
            if (blocksize < MINFILEBLOCK)
            {
                blocksize = MINFILEBLOCK;
            }

            IList<dynamic> jobResponses = new List<dynamic>();
            dynamic jobResponse=null;
            for (int i = 0; i < filepaths.Count; i++)
            {
                if (i % blocksize == 0)
                {
                    // Create job
                    try
                    {
                        jobResponse = _endpointUri
                        .AppendPathSegments("accounts", _account, "jobs")
                        .WithOAuthBearerToken(_token)
                        .PostJsonAsync(jobObj)
                        .ReceiveJson().Result;
                        // var jsonResponse = JsonConvert.SerializeObject(jobResponse);jo
                        // Add to responses
                        jobResponses.Add(Clone(jobResponse));
                    }
                    catch (Exception e)
                    {
                        // TODO logging
                        // Unwrapping
                        if (e is System.AggregateException)
                        {
                            foreach (var ie in ((System.AggregateException)e).InnerExceptions)
                            {
                                if (ie is FlurlHttpException)
                                {
                                    string msg = ((FlurlHttpException)ie).GetResponseStringAsync().Result;
                                    ie.HelpLink = msg;
                                }
                            }
                        }
                        e.HelpLink = $"failure to create job starting with file {filepaths[i]} with blocksize {blocksize}";
                        throw (e);
                    }
                }

                // Send file
                try
                {
                    var fileContent = new StringContent(File.ReadAllText(filepaths[i]));
                    //            var fileContent = new ByteArrayContent(File.ReadAllBytes(filepath));
                    var fileResponse = new Url(_baseUri + jobResponse.links[0].url)
                    .WithHeader("Content-type", "text/csv")
                    .WithOAuthBearerToken(_token)
                    .PostAsync(fileContent)
                    .ReceiveJson().Result;

                    // Add to responses
                    jobResponses.Add(fileResponse);
                }
                catch (Exception e)
                {
                    // TODO logging
                    // Unwrapping
                    if (e is System.AggregateException)
                    {
                        foreach (var ie in ((System.AggregateException)e).InnerExceptions)
                        {
                            if (ie is FlurlHttpException)
                            {
                                string msg = ((FlurlHttpException)ie).GetResponseStringAsync().Result;
                                ie.HelpLink = msg;
                            }
                        }
                    }
                    e.HelpLink = $"failure to send file {filepaths[i]}";
                    throw (e);
                }

                if (i % blocksize == (blocksize - 1) || i == (filepaths.Count - 1))
                {
                    try
                    {
                        // Complete files ingest job
                        var complJobObj = new Job<T>();

                        complJobObj.jobType = JOBTYPE.INGESTDATA;
                        complJobObj.status = JOBSTATUS.COMPLETED;
                        complJobObj.datastream = jobObj.datastream;
                        complJobObj.spec = jobObj.spec;
                        jobResponse = _endpointUri
                        .AppendPathSegments("accounts", _account, "jobs", (string)jobResponse.id)
                        .WithOAuthBearerToken(_token)
                        .PutJsonAsync(complJobObj)
                        .ReceiveJson().Result;

                        // Add to responses
                        jobResponses.Add(Clone(jobResponse));
                    }
                    catch (Exception e)
                    {
                        // TODO logging
                        // Unwrapping
                        if (e is System.AggregateException)
                        {
                            foreach (var ie in ((System.AggregateException)e).InnerExceptions)
                            {
                                if (ie is FlurlHttpException)
                                {
                                    string msg = ((FlurlHttpException)ie).GetResponseStringAsync().Result;
                                    ie.HelpLink = msg;
                                }
                            }
                        }
                        e.HelpLink = $"failure to close the job {(string)jobResponse.id} starting with file {filepaths[i - (blocksize - 1)]} with blocksize {blocksize}";
                        throw (e);
                    }
                }
            }

            return jobResponses;
        }

        public IList<dynamic> GetJobs()
        {
            // Get list ofjobs            
            var jobs = _endpointUri
            .AppendPathSegments("accounts", _account, "jobs")
            .SetQueryParams(new { limit = MAXJOBS })
            .WithOAuthBearerToken(_token)
            .GetJsonListAsync().Result;

            return jobs;
        }


        public IList<dynamic> GetUsers()
        {
            // Get list of users           
            var users = _endpointUri
            .AppendPathSegments("accounts", _account, "users")
            .SetQueryParams(new { limit = MAXUSERS })
            .WithOAuthBearerToken(_token)
            .GetJsonListAsync().Result;

            return users;
        }


        #endregion

        dynamic Clone(dynamic obj)
        {
            ExpandoObject ret = new ExpandoObject();
            if (obj is ExpandoObject)
            {
                ExpandoObject exp = (ExpandoObject)obj;
                foreach (var kvp in (IDictionary<string, object>)exp)
                {
                    ((IDictionary<string, object>)ret).Add(kvp);
                }
                return ret;
            }
            else
                throw (new ArgumentException("Only ExpandoObject types supported"));
        }
    }
}
