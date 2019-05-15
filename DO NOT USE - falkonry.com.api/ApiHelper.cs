using System;
using System.Collections.Generic;
using Flurl;
using Flurl.Http;
using System.Net.Http;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using System.Text;
using System.Net.Http.Headers;
// TODO: Logging

namespace falkonry.com.api
{
    public enum JOBTYPE
    {
        NONE,INGESTDATA // More to be added in the future
    }

    public enum JOBSTATUS
    {
        NONE,CREATED,
    }

    public interface IApiHelper
    {
        void PublishCSVFile<T>(string filepath,T format);
        IList<dynamic> GetDataStreams();
    }

    //{"jobType": "INGESTDATA", "status": "CREATED", "datastream": "1554938538981549", "spec": { "format": {"entityIdentifier": "person", "timeIdentifier": "time", "timeFormat": "YYYY-MMDD HH:mm:ss.SSS", "timeZone": "America/Los_Angeles" } } }
    //{"jobType": "INGESTDATA", "status": "CREATED", "datastream": "1554943865611097", "spec": { "format": { "signalIdentifier": "signal", "entityIdentifier": "entity", "valueIdentifier": "value", "timeIdentifier": "time", "timeFormat": "micros", "timeZone": "America/Los_Angeles" } } }' 
    public class StreamFormat
    {
        string entityIdentifier { get; set; }
        string timeIdentifier { get; set; }
        string timeFormat { get; set; }
        string timeZone { get; set; }
    }

    public class BatchFormat:StreamFormat
    {
        string batchIdentifier { get; set; }
    }

    public class NarrowStreamFormat:StreamFormat
    {
        string valueIdentifier { get; set; }
        string signalIdentifier { get; set; }
    }

    public class NarrowBatchStreamFormat:NarrowStreamFormat
    {
        string batchIdentifier { get; set; }
    }

    public class IngestSpec<T> where T:StreamFormat
    {
        public T format { get; set; }
    }

    public class Job<T> where T : StreamFormat
    {
        public JOBTYPE jobType { get; set; }
        public JOBSTATUS status { get; set; }
        public string datastream { get; set; }
        public IngestSpec<T> spec { get; set; }
    }

    public class ApiHelper : IApiHelper
    {
        static int MAXSTREAMS = 20;
        static int MAXASSESSMENTS = 10;
        static int MAXMODELS = 30;
        string _account = "";
        string _endpointUri = "https://localhost:30063/api";
        string _token = "";

        public ApiHelper(string account, string token, string endpointUri = "https://localhost:30063/api")
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
        public void PublishCSVFile<T>(string filepath,T jobObj)
        {
            // Create Job
            var jobInJson = JsonConvert.SerializeObject(jobObj);
            var json = new StringContent(jobInJson, Encoding.UTF8);
 //           json.Headers.ContentType = MediaTypeHeaderValue.Parse("application/json");

            //curl -X POST --header 'Content-Type: application/json' --header 'Authorization: Bearer [token]' -d '{ "jobType": "INGESTDATA", "status": "CREATED", "datastream": "1554938538981549", "spec": { "format": {"entityIdentifier": "person", "timeIdentifier": "time", "timeFormat": "YYYY-MMDD HH:mm:ss.SSS", "timeZone": "America/Los_Angeles" } } }' 'http://<server-address>:30063/api/1.1/accounts/1549746082718454/jobs'
            var jobResponse = _endpointUri
            .AppendPathSegments("accounts", _account, "jobs")
            .WithOAuthBearerToken(_token)
            .PostJsonAsync(jobObj)
            .ReceiveJson().Result;
            // Send data 
            //curl -X POST --header 'Content-type: text/csv' --header 'Authorization: Bearer [token]' --data-binary "@/Users/User1/Desktop/HumanActivity/source1.csv" 'http://<serveraddress>:30063/api/1.1/accounts/1549746082718454/datastreams/1554938538981549/ingestdata/1554942688319300/inputs'
/*
            var resp = await "http://api.com"
            .AppendPathSegments("accounts", _account, "datastreams")
            .SetQueryParams(new { limit = MAXSTREAMS })
            .WithOAuthBearerToken(_token)
            .GetJsonListAsync().Result;
            .PostMultipartAsync(mp => mp
            .AddString("name", "hello!")                // individual string
            .AddStringParts(new { a = 1, b = 2 })         // multiple strings
            .AddFile("file1", path1)                    // local file path
            .AddFile("file2", stream, "foo.txt")        // file stream
            .AddJson("json", new { foo = "x" })         // json
            .AddUrlEncoded("urlEnc", new { bar = "y" }) // URL-encoded                      
            .Add(content));                             // any HttpContent
*/
        }
    #endregion
    }
}
