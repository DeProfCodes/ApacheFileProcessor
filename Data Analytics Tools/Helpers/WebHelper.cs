using Data_Analytics_Tools.Models.APIResponse;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Data_Analytics_Tools.Helpers
{
    public class WebHelper
    {
        private HttpClient client;
        private string api_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VybmFtZV9zaGEiOiIwY2E1MGY3ZTZjZDIyNzc4NDg2YWZlZGI4Njg4ZTE0NmFlMTM2MjkwIn0.wV-rosnwbsFZUTomNnypVr1PN4gqwDpraIY54kIbF7w";
        private string requestUrl = "https://gnu0.azenqos.com/uapi";


        public WebHelper()
        {
            client = new HttpClient();
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));//ACCEPT header
        }

        private async Task<string> GetJsonAPIResponse(string apiCallEndPoint, string content, bool auth = false)
        {
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get,
                RequestUri = new Uri($"{requestUrl}/{apiCallEndPoint}"),
                Content = new StringContent(content, Encoding.UTF8, "application/json"),
            };

            if (auth)
            {
                request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    RequestUri = new Uri($"{requestUrl}/{apiCallEndPoint}"),
                    Headers = { { "Authorization", $"Bearer {api_token}" } },
                    Content = new StringContent(content, Encoding.UTF8, "application/json"),
                };
            }

            using (var response = await client.SendAsync(request))
            {
                response.EnsureSuccessStatusCode();
                var body = await response.Content.ReadAsStringAsync();
                return body;
            }
        }

        public async Task Curl()
        {
            try
            {//Telkom-SA
                //var content = "{\"Username\":\"trial_admin\",\"Password\":\"314isnotpina\"}";
                var content = "{\"Username\":\"Telkom-SA\",\"Password\":\"@2020sa\"}";
                var token = await GetJsonAPIResponse("login", content);
                bool done = true;
            }
            catch (Exception e)
            {
                int h = 0;
            }
        }

        private void SetLogsStartTime(List<ApacheLogHash> logsList)
        {
            for (int i = 0; i < logsList.Count; i++)
            {
                var time = logsList[i].LogOriFilename.Split()[1];
                var date = time.Split("_");
                logsList[i].StartDate = new DateTime(Convert.ToInt32(date[2]), Convert.ToInt32(date[1]), Convert.ToInt32(date[0]));
            }
        }

        public async Task<List<ApacheLogHash>> ListAllParquets(DateTime startDate, DateTime endDate)
        {
            try
            {
                var start_date = "\"" + startDate.ToString("yyyy-MM-dd") + "\"";
                var end_date = "\"" + endDate.AddDays(1).ToString("yyyy-MM-dd") + "\"";

                var content = "{\"start_time\":"+ start_date +",\"end_time\":"+ end_date + ", \"imei_list\":[]}";
                var logListJson = await GetJsonAPIResponse("list_logs", content, true);
            
                var logList = JsonSerializer.Deserialize<List<ApacheLogHash>>(logListJson);
                SetLogsStartTime(logList);

                logList = logList.Where(x => DateTime.Compare(x.StartDate, startDate) >= 0).ToList();

                var allTags = logList.Select(x => x.LogTag).Distinct().ToList();
                var allScriptTags = logList.Select(x => x.LogScriptName).Distinct().ToList();
                var find = logList.FirstOrDefault(x => x.LogHash == 3350746508903848522);
                int f = 4;
                return logList;
            }
            catch (Exception e)
            {
                int h = 0;
            }
            return null;
        }



















        /*OLD*/
        public async Task Cur2l()
        {
            try
            {
                var content = "{\"Username\":\"trial_admin\",\"Password\":\"314isnotpina\"}";
                await GetJsonAPIResponse("login", content);
            }
            catch (Exception e)
            {
                int h = 0;
            }

            try
            {
                // Create the HttpContent for the form to be posted.

                var username = "\"Username\":" + "\"trial_admin\"";
                var password = "\"Password\":" + "\"314isnotpina\"";

                var requestContent = new FormUrlEncodedContent(new[]
                {
                new KeyValuePair<string, string>("text", "{"+ $"{username},{password}" + "}"),
            });

                HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Post, "relativeAddress");
                var r = new StringContent("{\"Username\":\"trial_admin\",\"Password\":\"314isnotpina\"}", Encoding.UTF8, "application/json");//CONTENT-TYPE header

                //client.BaseAddress = new Uri("https:/test0.azenqos.com/uapi/login");
                HttpResponseMessage response = await client.PostAsync("https://test0.azenqos.com/uapi/login", r);
                //HttpResponseMessage response = await client.SendAsync(request);

                // Get the response content.
                HttpContent responseContent = response.Content;

                // Get the stream of the content.
                using (var reader = new StreamReader(await responseContent.ReadAsStreamAsync()))
                {
                    // Write the output.
                    var token = await reader.ReadToEndAsync();
                    int w = 8;
                }
            }
            catch (Exception e)
            {
                int h = 0;
            }
        }
    }
}
