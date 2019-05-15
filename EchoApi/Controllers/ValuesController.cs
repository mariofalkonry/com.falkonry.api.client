using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

namespace EchoApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class EchoController : ControllerBase
    {
        // GET api/echo/query?...
        [HttpGet("query")]
        public ActionResult<dynamic> Get()
        {
                var pairs = this.Request.Query;
                if (pairs.Count < 1)
                    return "are you serious?";
                dynamic ret = pairs;
                return ret;
        }

        // GET api/echo
        [HttpGet]
        public ActionResult<dynamic> Get(dynamic body)
        {
            return body;
        }

        // GET api/values/5
        //[HttpGet("{id}")]
        //public ActionResult<string> get(int id)
        //{
        //    return "value";
        //}

        // POST api/echo
        [HttpPost]
        public ActionResult<dynamic> Post(dynamic body)
        {
            return body;
        }

        // PUT api/values/5
        //[HttpPut("{id}")]
        //public void Put(int id, [FromBody] string value)
        //{
        //}

        // DELETE api/values/5
        //[HttpDelete("{id}")]
        //public void Delete(int id)
        //{
        //}
    }
}
