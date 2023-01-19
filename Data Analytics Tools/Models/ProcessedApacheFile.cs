namespace Data_Analytics_Tools.Models
{
    public class ProcessedApacheFile
    {
        public int Id { get; set; }
        public string Filename { get; set; }
        public bool ImportComplete { get; set; }
        public string ProcessError { get; set; }
    }
}
