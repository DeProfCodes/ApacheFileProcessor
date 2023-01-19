using System;
using System.Text.Json.Serialization;

namespace Data_Analytics_Tools.Models.APIResponse
{
    public class ApacheLogHash
    {
        [JsonPropertyName("log_hash")]
        public Int64 LogHash { get; set; }

        [JsonPropertyName("log_tag")]
        public string LogTag { get; set; }

        [JsonPropertyName("log_script_name")]
        public string LogScriptName { get; set; }

        [JsonPropertyName("log_ori_file_name")]
        public string LogOriFilename { get; set; }

        public DateTime StartDate { get; set; }

    }
}
