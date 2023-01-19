using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Data_Analytics_Tools.BusinessLogic;
using Data_Analytics_Tools.Constants;
using static System.Net.WebRequestMethods;

namespace Data_Analytics_Tools.Helpers
{
    public class ApacheLogFilesHelper
    {
        WebHelper api;
        SQL sql;
        ParquetFilesHelper parquets;
        IBusinessLogicData dataIO;
        WebClient webClient;
        Dictionary<string, List<Dictionary<string,string>>> schemas;

        private string apacheLogsDirectory;
        private string apacheLogsDirectory_LogHashes;
        private string apacheLogsDirectory_Downloads;

        public ApacheLogFilesHelper(IBusinessLogicData dataIO)
        {
            api = new WebHelper();
            sql = new SQL();
            parquets = new ParquetFilesHelper();
            webClient = new WebClient();

            this.dataIO = dataIO;

            schemas = new Dictionary<string, List<Dictionary<string,string>>>();

            SetApacheLogsDirectory(@"F:\Proficient\DATA\Apache Log Files");
        }

        #region Tables Schema
        public static string GetTableName(string query)
        {
            if (query.ToLower().Contains("insert") || query.ToLower().Contains("create"))
            {
                string table = "";
                int idx = query.IndexOf("\"");
                char t = query[++idx];
                while (t != '\"')
                {
                    table += t;
                    t = query[++idx];
                }
                return table;
            }
            return null;
        }

        public string ExtractTables(StreamReader file, List<string> Queries, ref string line)
        {
            line = file.ReadLine();
            string query = "";
            string queryBuilder = "";

            var currTableName = "";
            var currTableSchema = new List<Dictionary<string,string>>();
            Dictionary<string,string> dict;

            while (!line.Contains("}"))
            {
                var ls = line.Split("\t");
                if (ls.Length == 3 && (ls[2].ToLower() == "int," || ls[2].ToLower() == "integer,"))
                {
                    line = ls[0] + "\t" + ls[1] + "\t" + "BIGINT,";  //force all int tables to be bigint. cause: log_hash (int) sqlite sometimes too big for mysql (int)
                }                 
                if (ls.Length == 2 || (ls.Length == 3 && (ls[2].Trim() == "" || ls[2].Trim() == "," || ls[2].Trim().Length == 1)))
                {
                    if (ls.Length == 3 && ls[2] == ",")
                    {
                        line = line.Replace(",", "");
                    }

                    if(!ls[0].Contains("PRIMARY") && !ls[1].Contains("PRIMARY"))
                        line += "TEXT,";
                }
                if (line.Contains("TIMESTAMP"))
                {
                    line = line.Replace("TIMESTAMP", "DATETIME");
                }
                if (line.Contains("BLOB"))
                {
                    line = line.Replace("BLOB", "TEXT");
                }
                if (line.Contains("NUM"))
                {
                    line = line.Replace("NUM", "BIGINT");
                }

                ls = line.Split("\t");
                if (ls.Length == 3)
                {
                    var dtp = ls[2].Replace(",", "");
                    var fld = ls[1].Replace("\"", "");
                    dict = new Dictionary<string, string>();
                    dict.Add(fld, dtp);
                    currTableSchema.Add(dict);
                }

                queryBuilder += line + "\n";
                query += line + "\n";
                
                if (line == null || line.Contains("CREATE"))
                {
                    queryBuilder = queryBuilder.Replace("IF NOT EXISTS", "");
                    queryBuilder = queryBuilder.Replace("DEFAULT CURRENT_DATETIME", "");
                    queryBuilder = queryBuilder.Replace("AUTOINCREMENT", "");//
                    queryBuilder = queryBuilder.Replace("INTEGER_BOOLEAN", "BIT");
                    queryBuilder = queryBuilder.Replace("COMMIT;", "");

                    query = query.Replace("IF NOT EXISTS", "");
                    query = query.Replace("DEFAULT CURRENT_DATETIME", "");
                    query = query.Replace("AUTOINCREMENT", "");
                    query = query.Replace("INTEGER_BOOLEAN", "BIT");
                    query = query.Replace("COMMIT;", "");

                    Queries.Add(queryBuilder);
                    
                    var q = line != null ? line : queryBuilder;
                    var tableName = GetTableName(q);

                    if (tableName != null)
                    {
                        if (currTableName != "")
                        {
                            if(!schemas.ContainsKey(currTableName))
                                schemas.Add(currTableName.ToLower(), currTableSchema);
                            
                            currTableSchema = new List<Dictionary<string,string>>();
                        }
                        currTableName = tableName;
                    }

                    if (line == null)
                    {
                        break;
                    }
                    queryBuilder = "";
                }

                if (line.ToLower().Contains("insert"))
                {
                    if (currTableName != "" && currTableSchema.Count > 0)
                    {
                        if (!schemas.ContainsKey(currTableName))
                            schemas.Add(currTableName, currTableSchema);

                        currTableSchema = new List<Dictionary<string,string>>();
                    }
                    break;
                }

                line = file.ReadLine();
                if (line == null)
                    break;
            }

            if (currTableName != "" && currTableSchema.Count > 0)
            {
                if (!schemas.ContainsKey(currTableName))
                    schemas.Add(currTableName, currTableSchema);
            }

            query = query.Replace("IF NOT EXISTS", "");
            query = query.Replace("COMMIT;", "");

            return query;
        }

        public async Task CreateTablesSchema(bool schemaOnly=false)
        {
            await sql.CreateDatabase("Q3_2022");

            var schemaDir = "DATA\\Schema.txt";

            StreamReader file = new StreamReader(schemaDir);
            var createTablesQueries = new List<string>();

            string line = file.ReadLine();
            try
            {
                var createTables = ExtractTables(file, createTablesQueries, ref line);

                if (!schemaOnly)
                {
                    //await sql.RunBulkQueries(createTablesQueries);
                    await sql.RunQuery(createTables);
                }
            }
            catch (Exception e)
            {
                int t = 5;
            }
        }

        public void SetApacheLogsDirectory(string directory)
        {
            apacheLogsDirectory = directory;
            apacheLogsDirectory_LogHashes = apacheLogsDirectory + @"\Log hashes\";
            apacheLogsDirectory_Downloads = apacheLogsDirectory + @"\Downloads\";
        }
        #endregion

        #region Downloading Apache Logs
        public async Task CreateLogFileListForDownload()
        {
            string logHashesListFile = apacheLogsDirectory_LogHashes + "log_hash_keys.txt";
            string hashListFileForDownloadDir = apacheLogsDirectory_LogHashes + "log_fileList_for_download.txt";

            StreamReader hashesFile = new StreamReader(logHashesListFile);
            StreamWriter hashListFileForDownload = new StreamWriter(hashListFileForDownloadDir);
            
            string azenqosPrefix = "https://gnu0.azenqos.com/logs";
            var tables = ApacheConstants.GetApacheKnownTables();

            //var logHashes = (hashesFile.ReadToEnd()).Split(new char[] {','}, StringSplitOptions.RemoveEmptyEntries);
            var startDate = new DateTime(2022, 11, 01);
            var endDate = DateTime.Now;//new DateTime(2022, 11, 30);
            var logHashes = (await api.ListAllParquets(startDate, endDate)).Select(x=>x.LogHash).ToList();

            hashListFileForDownload.Flush();
            foreach (var table in tables)
            {
                foreach(var logHash in logHashes)
                {
                    var downloadLine = $"{azenqosPrefix}/{2022}_{11.ToString("D2")}/{table}_{logHash}.parquet" + ",";
                    hashListFileForDownload.WriteLine(downloadLine);
                }
            }

            hashesFile.Close();
            hashListFileForDownload.Close();
        }

        private string GetApacheFileName(string apacheLink)
        {
            var data = apacheLink.Split("/");
            var fileName = data[data.Length - 1];

            return fileName;
        }

        private bool DownloadApacheFileFromServer(string fileUrl, List<string> errorsList)
        {
            try
            {
                var destination = apacheLogsDirectory_Downloads + GetApacheFileName(fileUrl);
                webClient.DownloadFile(fileUrl, destination);
                
                return true;
            }
            catch (Exception e)
            {
                errorsList.Add(e.Message);
            }
            return false;
        }

        public void DownloadApacheFilesFromServer()
        {
           // await api.ListAllParquets();

            string logfileList = apacheLogsDirectory_LogHashes + "log_fileList_for_download.txt";
            StreamReader file = new StreamReader(logfileList);

            var allApacheLinks = file.ReadToEnd().Split(",");
            file.Close();

            var errorsList = new List<string>();
            
            foreach (var apacheLink in allApacheLinks)
            {
               DownloadApacheFileFromServer(apacheLink, errorsList);    
            }
        }
        #endregion

        #region Apache Files To MySQL
        private async Task DeleteFileProceedFiles()
        {
            var processFiles = await dataIO.GetProcessedApacheFiles();
            var deleted = new List<string>();
            foreach (var file in processFiles)
            {
                try
                {
                    if (file != "")
                    {
                        System.IO.File.Delete(file);
                        deleted.Add(file);  
                    }
                }
                catch (Exception e)
                {

                }
            }
            await dataIO.DeleteApacheLogFileImport(deleted);
        }

        public async Task<int> ImportApacheFileToMySQL(string file, List<string> errors)
        {
            try
            {
                var insertQueries = await parquets.CreateMySQLInsertQueries(file, schemas);
                int rows = await sql.RunQuery(insertQueries);

                if (rows > 0)
                {
                    await dataIO.AddOrUpdateApacheLogFileImport(file, true, "");
                }
                else
                {
                    await dataIO.AddOrUpdateApacheLogFileImport(file, false, "");
                }
                return rows;
            }
            catch (Exception e)
            {
                await dataIO.AddOrUpdateApacheLogFileImport(file, false, e.Message);
                errors.Add(e.Message);
            }
            return -1;
        }

        public async Task ImportApacheFilesToMySQL()
        {
            var location = apacheLogsDirectory_Downloads;

            var apacheFiles = Directory.GetFiles(location);
            
            int successCount = 0;

            var errors = new List<string>();
            
            foreach (var afile in apacheFiles)
            {
                int rows = await ImportApacheFileToMySQL(afile, errors);

                successCount += rows > 0 ? 1 : 0;
            }
        }

        public async Task DownloadAndImportApacheFilesToMySQL()
        {
            string logfileList = apacheLogsDirectory_LogHashes + "log_fileList_for_download.txt";
            StreamReader file = new StreamReader(logfileList);

            var allApacheLinks = file.ReadToEnd().Split(",");
            file.Close();

            int downloadsCount = 0;
            int importedFilesCount = 0;

            var errorsList = new List<string>();

            foreach (var apacheLink in allApacheLinks)
            {
                bool downloaded = DownloadApacheFileFromServer(apacheLink, errorsList);
                if (downloaded)
                {
                    int rows = await ImportApacheFileToMySQL(apacheLink, errorsList);

                    downloadsCount++;
                    importedFilesCount += rows > 0 ? 1 : 0;
                }
            }
        }


        #endregion
    }
}
