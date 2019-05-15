using System;
using System.IO;
using System.Text;
using Flurl;
using Flurl.Http;
using System.Net.Http;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;

namespace FlurSandbox
{
    class Program
    {
        static int MAXSTREAMS = 20;
        static int MAXASSESSMENTS = 10;
        static int MAXMODELS = 30;
        // Falkonry App2
        //        static string api = "https://app2.falkonry.ai:30063/api/1.1";
        // Mario Brenes Working Account 
        //        static string account = "lpwouijrq82p6o";
        //  Unknown Dataservice Token?
        //        static string token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE4NzA5ODgwNTUsICJlbWFpbCIgOiAibWFyaW8uYnJlbmVzQGZhbGtvbnJ5LmNvbSIsICJuYW1lIiA6ICJtYXJpby5icmVuZXNAZmFsa29ucnkuY29tIiwgInNlc3Npb24iIDogIjE1NTkwNTE0MDgxNjI5NDUiIH0.9Uzt0xeT4n5fhmQndwl5PjW69RiT5SAaB_6RZRuG2eE";
        // mario.brenes@falkonry.com DataService Token
        //static string token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE4NzA5ODkwOTYsICJlbWFpbCIgOiAibWFyaW8uYnJlbmVzQGZhbGtvbnJ5LmNvbSIsICJuYW1lIiA6ICJtYXJpby5icmVuZXNAZmFsa29ucnkuY29tIiwgInNlc3Npb24iIDogIjE1NTkwNTM0OTAzNjY1NjUiIH0._q5ZDvLlFi9blxBkk7dgcKaBej0zphLrxuBXIg4Kl0k";
        // namrata.rao@falkonry.com Dataservice Token?
        //static string token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE4NzA5ODg5NzYsICJlbWFpbCIgOiAibmFtcmF0YS5yYW9AZmFsa29ucnkuY29tIiwgIm5hbWUiIDogIm5hbXJhdGEucmFvQGZhbGtvbnJ5LmNvbSIsICJzZXNzaW9uIiA6ICIxNTU5MDUzMjUxMTY5MTAzIiB9.LFxVQrFxH13LC4duXjskKsV9OO_OSMzhfAG1PdJdfOE";
        // Burkhart Prognost at Falkonry
        //        static string datastream = "k8lrgpgwrtp9bl"
        // Teekay
        // static string api = "http://168.63.24.204:30063/api/1.1";
        static string api = "https://168.63.24.204/api/1.1"; // Skipping port for now
        static string account = "Lsosgyy0b8cynn";
        static string token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE4NzA5ODE4OTAsICJlbWFpbCIgOiAibWFyaW8uYnJlbmVzQGZhbGtvbnJ5LmNvbSIsICJuYW1lIiA6ICJtYXJpby5icmVuZXNAZmFsa29ucnkuY29tIiwgInNlc3Npb24iIDogIjE1NTU4OTUzMDY0NjY4NDUiIH0.s_cBIkUQbybLvMix9xCVC6lfkSeYrxMdFlFGmNI8MNQ";

        static void Main(string[] args)
        {
            Console.WriteLine("Accessing Falkonry API!");


            // Get list of data streams            
            var streams = api
            .AppendPathSegments("accounts", account, "datastreams")
            .SetQueryParams(new { limit = MAXSTREAMS })
            .WithOAuthBearerToken(token)
            .GetJsonListAsync().Result;

            // For each datastream get assessments
            foreach (var stream in streams)
            {
                // Get list of assessments
                string streamId = stream.id;
                stream.assessments = api.AppendPathSegments("accounts", account, "datastream",streamId,"assessments")
                    .SetQueryParams(new { limit = MAXASSESSMENTS })
                    .WithOAuthBearerToken(token)
                    .GetJsonListAsync().Result;

                // For each assessment get models
                foreach (var assessment in stream.assessments)
                {
                    // Get list of models
                    string assessmentId = assessment.id;
                    assessment.models=api.AppendPathSegments("accounts", account, "datastream",streamId, "assessment",assessmentId,"models")
                        .SetQueryParams(new { limit = MAXMODELS })
                        .WithOAuthBearerToken(token)
                        .GetJsonListAsync().Result;
                }
            }



            Console.WriteLine("Worked!");
        }


        static string GetTextResponse(HttpResponseMessage msg)
        {
            Stream receiveStream = msg.Content.ReadAsStreamAsync().Result;
            StreamReader readStream = new StreamReader(receiveStream, Encoding.UTF8);
            return readStream.ReadToEnd();
        }

        static JArray GetJObject(HttpResponseMessage msg)
        {
            var txt = GetTextResponse(msg);
            return JArray.Parse(txt);
        }

    }
}