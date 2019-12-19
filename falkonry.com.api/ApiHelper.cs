/*
The MIT License
Copyright © 2010-2019 Falkonry.com
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

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
using System.Threading;
using System.Linq;
using falkonry.com.util;
// TODO: Logging

namespace falkonry.com.api
{
    public enum JOBTYPE
    {
        NONE,INGESTDATA,LIVE, 
         LEARN, APPLY, INGESTEVENTS, DIGEST, DATAPERSIST, IMAGE, INVITE, EXPORT, TESTEMAIL, EXPLANATIONREPORT, REORG
        // More to be added in the future
    }

    public enum JOBSTATUS
    {
        NONE,CREATED,COMPLETED,CANCELLED,RUNNING,FAILED
    }

    public enum MODELTYPE
    {
        BATCH,SLIDING_WINDOW
    }

    public interface IApiHelper
    {
        IList<dynamic> LoadCSVFile<T>(string filepath,Job<T> job,uint chunkMB=0) where T:StreamFormat;

        IList<dynamic> LoadCSVFiles<T>(IList<string> filepaths, Job<T> job,int blocksize=20,int sleepSecs=20, uint chunkMB=0) where T : StreamFormat;

        IList<dynamic> GetDataStreams();
        IList<dynamic> GetJobs(string stream = null, IList<JOBTYPE> types = null, IList<JOBSTATUS> statuses = null);
        dynamic CancelJob(string jobId);
        IList<dynamic> CancelStreamJobs(string stream,List<JOBTYPE> typeFilters,List<JOBSTATUS> statusFilters);

        IList<dynamic> GetUsers();

        dynamic SetDataStream(string Name, string timezone, MODELTYPE modelType,string baseTimeUnit="micros");

        // TODO: Is this Good?  I can get props for other accounts.  Maybe make like Jobs API?
        IList<dynamic> SetAccountProps(string account,Dictionary<string, string> props); 
        IList<dynamic> GetAccountProps(string account);

    }

    //WIDE - {"jobType": "INGESTDATA", "status": "CREATED", "datastream": "1554938538981549", "spec": { "format": {"entityIdentifier": "person", "timeIdentifier": "time", "timeFormat": "YYYY-MMDD HH:mm:ss.SSS", "timeZone": "America/Los_Angeles" } } }
    //NARROW - {"jobType": "INGESTDATA", "status": "CREATED", "datastream": "1554943865611097", "spec": { "format": { "signalIdentifier": "signal", "entityIdentifier": "entity", "valueIdentifier": "value", "timeIdentifier": "time", "timeFormat": "micros", "timeZone": "America/Los_Angeles" } } }' 
    public class StreamFormat
    {
        public string entityIdentifier { get; set; }
        public string timeIdentifier { get; set; }
        public string timeFormat { get; set; }

        // Java TimeZone names (see https://docs.oracle.com/javase/8/docs/api/java/time/ZoneId.html)
        public string timeZone { get; set; }
        public string entityKey { get; set; }  // When no entity identifier is provided


        public static StreamFormat CreateFromFile(string path)
        {
            if (string.IsNullOrEmpty(path))
                throw (new ArgumentNullException(nameof(path)));

            string configPath = path.Contains(Path.DirectorySeparatorChar) ? path : $"{AppDomain.CurrentDomain.BaseDirectory}{path}";
            var jsonString = File.ReadAllText(configPath);
            try
            {
                var streamObj = JsonConvert.DeserializeObject<NarrowBatchFormat>(jsonString);

                // Test that must have entityKey or entityIdentifier
                if (String.IsNullOrEmpty(streamObj.entityIdentifier) && String.IsNullOrEmpty(streamObj.entityKey))
                    throw (new ArgumentException("Must specify an entityIdentifier column or an entityKey"));

                dynamic newObj=null;
                // Not narrow
                if (String.IsNullOrEmpty(streamObj.signalIdentifier))
                {
                    // Not batch
                    if (String.IsNullOrEmpty(streamObj.batchIdentifier))
                    {
                        // No entityKey
                        if (String.IsNullOrEmpty(streamObj.entityKey))
                        {
                            newObj = new StreamFormat
                            {
                                entityIdentifier = streamObj.entityIdentifier,
                                timeIdentifier = streamObj.timeIdentifier,
                                timeFormat = streamObj.timeFormat,
                                timeZone = streamObj.timeZone

                            };
                        }
                        else
                        {
                            newObj = new StreamFormat
                            {
                                entityKey = streamObj.entityKey,
                                timeIdentifier = streamObj.timeIdentifier,
                                timeFormat = streamObj.timeFormat,
                                timeZone = streamObj.timeZone
                            };
                        }
                    }
                    else
                    {
                        // No entityKey
                        if (String.IsNullOrEmpty(streamObj.entityKey))
                        {
                            newObj = new BatchFormat
                            {
                                batchIdentifier = streamObj.batchIdentifier,
                                entityIdentifier = streamObj.entityIdentifier,
                                timeIdentifier = streamObj.timeIdentifier,
                                timeFormat = streamObj.timeFormat,
                                timeZone = streamObj.timeZone

                            };
                        }
                        else
                        {
                            newObj = new BatchFormat
                            {
                                batchIdentifier = streamObj.batchIdentifier,
                                entityKey = streamObj.entityKey,
                                timeIdentifier = streamObj.timeIdentifier,
                                timeFormat = streamObj.timeFormat,
                                timeZone = streamObj.timeZone
                            };
                        }
                    }
                }
                else
                {
                    // Not batch
                    if (String.IsNullOrEmpty(streamObj.batchIdentifier))
                    {
                        if(String.IsNullOrEmpty(streamObj.entityKey))
                        {
                            newObj = new NarrowFormat
                            {
                                entityIdentifier = streamObj.entityIdentifier,
                                timeIdentifier=streamObj.timeIdentifier,
                                timeFormat=streamObj.timeFormat,
                                timeZone=streamObj.timeZone,
                                signalIdentifier = streamObj.signalIdentifier,
                                valueIdentifier=streamObj.valueIdentifier
                            };
                        }
                        else
                        {
                            newObj = new NarrowFormat
                            {
                                entityKey = streamObj.entityKey,
                                timeIdentifier = streamObj.timeIdentifier,
                                timeFormat = streamObj.timeFormat,
                                timeZone = streamObj.timeZone,
                                signalIdentifier = streamObj.signalIdentifier,
                                valueIdentifier = streamObj.valueIdentifier
                            };
                        }
                    }
                }
                return newObj??streamObj;
            }
            catch(Exception e)
            {
                throw (e);
            }
        }
    }

    public class DataStreamSpec
    {
        public string name { get; set; }
        public string timeZone { get; set; }
        public bool isBatch { get; set; }
        public string baseTimeUnit { get; set; }
    }

    public class BatchFormat:StreamFormat
    {
        public string batchIdentifier { get; set; }
    }

    public class NarrowFormat:StreamFormat
    {
        public string valueIdentifier { get; set; }
        public string signalIdentifier { get; set; }
    }

    public class NarrowBatchFormat:NarrowFormat
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
        static int MAXPROPS = 100;
        static int MINSLEEPSEC = 0;
        static int MINFILEBLOCK = 2;
        static int MAXSTREAMS = 100;
        static int MAXASSESSMENTS = 50;
        static int MAXMODELS = 100;
        static int MAXJOBS = 30;
        static int MAXUSERS = 20;
        string _account = "";
        string _endpointUri = "https://localhost:30063/api/1.1";
        string _token = "";
        string _baseUri = "";
        Action<dynamic> _responseCallback; // Used to notify of multi-response calls

        public ApiHelper(string account, string token, string endpointUri = "https://localhost:30063/api/1.1", Dictionary<string, string> accountProps = null, Action<dynamic> responseCallback = null)
        {
            _responseCallback = responseCallback;
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
            var idx = endpointUri.IndexOf("/api/");
            if (idx<0)
                throw (new ArgumentException($"Uri {endpointUri} is not valid"));
            _baseUri = _endpointUri.Substring(0, idx);

            // TODO: remove this later
            // Configure to ignore bad certificates for this endpoint
            FlurlHttp.ConfigureClient(_endpointUri, cli =>
                {
                    cli.Settings.HttpClientFactory = new UntrustedCertClientFactory();
                    cli.Settings.Timeout = new TimeSpan(0, 30, 0);  // 30 minutes timeout
                });

            // Configure account properties if any
            if (accountProps != null && accountProps.Count>0)
            {
                var response = SetAccountProps(account,accountProps);
            }
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
        public IList<dynamic> GetAccountProps(string account)
        {
            // Get list of props            
            var props = _endpointUri
            .AppendPathSegments("accounts", _account, "properties")
            .SetQueryParams(new { limit = MAXPROPS })
            .WithOAuthBearerToken(_token)
            .GetJsonAsync().Result;
            return props.properties;
        }

        public IList<dynamic> SetAccountProps(string account, Dictionary<string, string> props)
        {
            // Get existing properties
            var existProps = GetAccountProps(account);
            List<dynamic> responses = new List<dynamic>();
            foreach (var kv in props)
            {
                var exist = existProps.Where(p => p.key == kv.Key).FirstOrDefault();
                var prop = new
                {
                    key = kv.Key,
                    value = kv.Value
                };

                try
                {
                    // Create
                    dynamic propResponse = null;
                    if (exist == null)
                    {
                        propResponse = _endpointUri
                        .AppendPathSegments("accounts", _account, "properties")
                        .WithOAuthBearerToken(_token)
                        .PostJsonAsync(prop)
                        .ReceiveJson().Result;
                    }
                    else
                    {
                        propResponse = _endpointUri
                        .AppendPathSegments("accounts", _account, "properties", (string)exist.id)
                        .WithOAuthBearerToken(_token)
                        .PutJsonAsync(prop)
                        .ReceiveJson().Result;
                    }
                    _responseCallback.Invoke(propResponse);
                    responses.Add(propResponse);
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
                    e.HelpLink = $"failure to set account property {JsonConvert.SerializeObject(prop)}";
                    throw (e);
                }
            }
            return responses;

        }

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
        public IList<dynamic> LoadCSVFile<T>(string filepath, Job<T> jobObj,uint chunkMB=0) where T : StreamFormat
        {
            // Create Job
            //var jobInJson = JsonConvert.SerializeObject(jobObj);
            var jobResponses=new List<dynamic>();
            try
            {
                var jobResponse = _endpointUri
                .AppendPathSegments("accounts", _account, "jobs")
                .WithOAuthBearerToken(_token)
                .PostJsonAsync(jobObj)
                .ReceiveJson().Result;
                jobResponses.Add(jobResponse);
                // var jsonResponse = JsonConvert.SerializeObject(jobResponse);

                // Loop sending chunks
                // Send data
                //curl -X POST --header 'Content-type: text/csv' --header 'Authorization: Bearer [token]' --data-binary "@/Users/User1/Desktop/HumanActivity/source1.csv" 'http://<serveraddress>:30063/api/1.1/accounts/1549746082718454/datastreams/1554938538981549/ingestdata/1554942688319300/inputs
                using (var reader = File.OpenText(filepath))
                {
                    // If no chunking send entire content at once
                    if (chunkMB <= 0)
                    {
                        var fileContent = new StringContent(File.ReadAllText(filepath));
                        var fileResponse = new Url(_baseUri + jobResponse.links[0].url)
                        .WithHeader("Content-type", "text/csv")
                        .WithOAuthBearerToken(_token)
                        .PostAsync(fileContent)
                        .ReceiveJson().Result;
                        jobResponses.Add(fileResponse);
                    }
                    else
                    {
                        Action<String> sendText = delegate (String text)
                        {
                            var chunkContent = new StringContent(text);
                            var chunkResponse = new Url(_baseUri + jobResponse.links[0].url)
                            .WithHeader("Content-type", "text/csv")
                            .WithOAuthBearerToken(_token)
                            .PostAsync(chunkContent)
                            .ReceiveJson().Result;
                            jobResponses.Add(chunkResponse);
                        };
                        Chunker.ChunkTextFromText(Chunker.GetChunkSize(chunkMB), reader, sendText);
                    }
                }

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
                jobResponses.Add(jobResponse);
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
            return jobResponses;
        }

        public IList<dynamic> LoadCSVFiles<T>(IList<string> filepaths, Job<T> jobObj, int blocksize=0,int sleepSecs=0,uint chunkMB=0) where T : StreamFormat
        {
            if(blocksize==0)
            {
                blocksize = filepaths.Count;
            }
            else if (blocksize < MINFILEBLOCK)
            {
                blocksize = MINFILEBLOCK;
            }
            if(sleepSecs<MINSLEEPSEC)
            {
                sleepSecs = MINSLEEPSEC;
            }

            List<dynamic> jobResponses = new List<dynamic>();
            dynamic jobResponse=null;
            var jobid = "";
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
                        // var jsonResponse = JsonConvert.SerializeObject(jobResponse);
                        jobid = jobResponse.id;
                        var clone = Clone(jobResponse);
                        // Callback
                        _responseCallback?.Invoke(clone);
                        // Add to responses
                        jobResponses.Add(clone);
                    }
                    catch (Exception e)
                    {
                        // TODO logging
                        // Unwrappingnes
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
                        var excResponse = CreateExceptionResponse(e, "creating job");
                        // Call callback
                        _responseCallback?.Invoke(excResponse);
                        jobResponses.Add(excResponse);;
                        break;
                    }
                }

                // Send file
                var start = DateTime.Now;
                try
                {
                    var info = new FileInfo(filepaths[i]);
                    var size = info.Length;
                    Console.WriteLine($"Sending file {filepaths[i]} of {info.Length} bytes" + (chunkMB>0?$", in chunks of {chunkMB} MB":""));
                    var fileResponses = new List<dynamic>();
                    // Loop sending chunks
                    // Send data
                    //curl -X POST --header 'Content-type: text/csv' --header 'Authorization: Bearer [token]' --data-binary "@/Users/User1/Desktop/HumanActivity/source1.csv" 'http://<serveraddress>:30063/api/1.1/accounts/1549746082718454/datastreams/1554938538981549/ingestdata/1554942688319300/inputs
                    using (var reader = File.OpenText(filepaths[i]))
                    {
                        // If no chunking send entire content at once
                        if (chunkMB <= 0)
                        {
                            var fileContent = new StringContent(File.ReadAllText(filepaths[i]));
                            var fileResponse = new Url(_baseUri + jobResponse.links[0].url)
                            .WithHeader("Content-type", "text/csv")
                            .WithOAuthBearerToken(_token)
                            .PostAsync(fileContent)
                            .ReceiveJson().Result;
                            fileResponses.Add(fileResponse);
                            // Call callback
                            _responseCallback?.Invoke(fileResponse);
                        }
                        else
                        {
                            Action<String> sendText = delegate (String text)
                            {
                                var chunkContent = new StringContent(text);
                                var chunkResponse = new Url(_baseUri + jobResponse.links[0].url)
                                .WithHeader("Content-type", "text/csv")
                                .WithOAuthBearerToken(_token)
                                .PostAsync(chunkContent)
                                .ReceiveJson().Result;
                                fileResponses.Add(chunkResponse);
                                // Call callback
                                _responseCallback?.Invoke(chunkResponse);
                            };
                            Chunker.ChunkTextFromText(Chunker.GetChunkSize(chunkMB), reader, sendText);
                        }
                    }

                    // TODO: Move to callbacks
                    dynamic stats = new ExpandoObject();
                    stats.jobid = jobid;
                    stats.file = filepaths[i];
                    stats.time = TimeSpan.FromTicks(DateTime.Now.Ticks - start.Ticks);
                    Console.WriteLine($"It took {stats.time} to send file {stats.file}");

                    // Add to responses
                    jobResponses.Add(stats);
                    jobResponses.AddRange(fileResponses);

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
                    e.HelpLink = $"failure to send file {filepaths[i]} after {TimeSpan.FromTicks(DateTime.Now.Ticks-start.Ticks)}";
                    var excResponse = CreateExceptionResponse(e, "sending file");

                    // Call callback for exception thrown
                    _responseCallback?.Invoke(excResponse);
                    
                    jobResponses.Add(excResponse);
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

                        // Call callback
                        _responseCallback?.Invoke(jobResponse);

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
                        var excResponse = CreateExceptionResponse(e, "closing job");

                        // Call callback
                        _responseCallback?.Invoke(excResponse);

                        // Add to responses
                        jobResponses.Add(excResponse);

                        break;
                    }

                    if (sleepSecs>0)
                        Thread.Sleep(sleepSecs * 1000);

                }
            }
            return jobResponses;
        }

        public dynamic SetDataStream(string name, string timezone, MODELTYPE modelType, string baseTimeUnit = "micros")
        {
            // Check if already exists
            var existStreams = GetDataStreams();
            var exist = existStreams.Where(p => p.name == name);
            if (exist.Any())
            {
                if (exist.Count() > 1)
                {
                    throw (new ArgumentException($"More than one stream match the name ${name}"));
                }
                return exist.First();
                /*  TODO: Why does this throw runtime binding exception
                    exist.First().id,
                    message = "Stream already exists"
                };
                */
            }

            DataStreamSpec spec = new DataStreamSpec()
            {
                name = name,
                timeZone = timezone,
                isBatch = modelType == MODELTYPE.BATCH,
                baseTimeUnit = baseTimeUnit
            };

            try
            {
                var streamResponse = _endpointUri
                .AppendPathSegments("accounts", _account, "datastreams")
                .WithOAuthBearerToken(_token)
                .PostJsonAsync(spec)
                .ReceiveJson().Result;
                return streamResponse;
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
                e.HelpLink = $"failure to create datastream";
                throw (e);
            }
        }

        public IList<dynamic> GetJobs(string stream=null,IList<JOBTYPE> types=null,IList<JOBSTATUS> statuses=null)
        {
            // Get list ofjobs           
            IList<dynamic> jobs = new List<dynamic>();
            var request = _endpointUri
                .AppendPathSegments("accounts", _account, "jobs")
                .WithOAuthBearerToken(_token)
                .SetQueryParam("limit", MAXJOBS);
            if(!String.IsNullOrEmpty(stream))
                request = request.SetQueryParam("datastream", new[] { stream });
            if(types!=null && types.Count > 0)
            {
                var t = (from type in types select type.ToString()).ToArray();
                request = request.SetQueryParam("type",t);
            }
            if(statuses!=null && statuses.Count>0)
            {
                var s = (from status in statuses select status.ToString()).ToArray();
                request = request.SetQueryParam("status",s);
            }
            jobs =request.GetJsonListAsync().Result;
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

        public dynamic CancelJob(string jobId)
        {
            dynamic response = null;
            try
            {
                // Get job to see if it exists else return exception
                var exists = _endpointUri
                .AppendPathSegments("accounts", _account, "jobs",jobId)
                .SetQueryParams(new { limit = MAXPROPS })
                .WithOAuthBearerToken(_token)
                .GetJsonAsync().Result;

                // If stream does not exists, must delete  instead of cancel
                var streamExists= from stream in GetDataStreams() where stream.id == exists.datastream select stream;
                if (!streamExists.Any())
                {
                    response = _endpointUri
                    .AppendPathSegments("accounts", _account, "jobs", jobId)
                    .WithOAuthBearerToken(_token)
                    .DeleteAsync()
                    .ReceiveJson().Result;
                }
                else
                {
                    // Cancel job
                    /*
                    var jsonText = JsonConvert.SerializeObject(exists, Formatting.Indented,
                                       new JsonSerializerSettings
                                       {
                                           ReferenceLoopHandling = ReferenceLoopHandling.Ignore
                                           //ReferenceLoopHandling=ReferenceLoopHandling.Serialize,
                                           //PreserveReferencesHandling=PreserveReferencesHandling.Objects
                                       });
                    */
                    var cancelJobObj = new Job<StreamFormat>();
                    cancelJobObj.jobType = (JOBTYPE)Enum.Parse(typeof(JOBTYPE), exists.jobType);
                    cancelJobObj.status = JOBSTATUS.CANCELLED;
                    cancelJobObj.datastream = exists.datastream;
                    // Cannot leave spec undefined so fake it
                    bool hasEntityId = ((IDictionary<string, object>)exists.spec.format).ContainsKey("entityIdentifier");
                    cancelJobObj.spec = new IngestSpec<StreamFormat>()
                    {
                        format = new StreamFormat()
                        {
                            entityIdentifier = hasEntityId?exists.spec.format.entityIdentifier:"",
                            entityKey=!hasEntityId?exists.spec.format.entityKey:"",
                            timeIdentifier = exists.spec.format.timeIdentifier,
                            timeFormat = exists.spec.format.timeFormat,
                        }   
                    };
                    var jobInJson = JsonConvert.SerializeObject(cancelJobObj); // For debugging 
                    response = _endpointUri
                    .AppendPathSegments("accounts", _account, "jobs", jobId)
                    .WithOAuthBearerToken(_token)
                    .PutJsonAsync(cancelJobObj)
                    .ReceiveJson().Result;
                }
            }
            catch (Exception e)
            {
                // Unwrappingnes
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
                e.HelpLink = $"failure to cancel job with id {jobId}";
                var excResponse = CreateExceptionResponse(e, "cancelling job");
                // Call callback
                _responseCallback?.Invoke(excResponse);
                throw (e);
            }
            return response;
        }

        public IList<dynamic> CancelStreamJobs(string stream, List<JOBTYPE> typeFilters, List<JOBSTATUS> statusFilters)
        {
            IList<dynamic> existJobs = new List<dynamic>();
            IList<dynamic> cancelResponses = new List<dynamic>();
            // Read jobs
            try 
            {
                // Get jobs
                existJobs = GetJobs(stream,typeFilters,statusFilters);
            }
            catch(Exception e)
            {
                // Unwrappingnes
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
                e.HelpLink = $"failure to query jobs for account {_account}";
                var excResponse = CreateExceptionResponse(e, "querying jobs");
                // Call callback
                _responseCallback?.Invoke(excResponse);
                throw (e);
            }

            // Cancel them
            foreach(var job in existJobs)
            {
                try
                {
                    var response=CancelJob(job.id);
                    cancelResponses.Add(response);
                }
                catch(Exception e)
                {
                    // No need to aggregate CancelJob did that already
                    var excResponse = CreateExceptionResponse(e, "cancelling jobs");
                    // Call callback
                    _responseCallback?.Invoke(excResponse);
                    // Add to returned list
                    cancelResponses.Add(excResponse);
                }
            }

            return cancelResponses;
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


        private dynamic CreateExceptionResponse(Exception e,string type)
        {
            var exceptObj = new
            {
                exception = e.Message,
                type,
                message = e.HelpLink,
                stack=e.StackTrace,
                inner= new List<dynamic>()
            };

            if (e is System.AggregateException)
            {
                foreach (var ie in ((System.AggregateException)e).InnerExceptions)
                {
                    var innerObj = new
                    {
                        exception = ie.Message,
                        message = ie.HelpLink,
                        stack = ie.StackTrace
                    };
                    exceptObj.inner.Add(innerObj);
                }
            }

            return exceptObj;
        }
    }
}
