using Parquet;
using Parquet.Data;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Data_Analytics_Tools.Helpers
{
    public class ParquetFilesHelper
    {
        public ParquetFilesHelper()
        {
                
        }

        private static string GetValue(DataColumn column, int col)
        {
            if (column != null)
            {
                var data = column.Data.GetValue(col);
                if (data is byte[])
                {
                    var bytes = (byte[])data;
                    return string.Join("", bytes);
                }
                else if (data is double? || data is float?)
                {
                    return data?.ToString().Replace(",", ".") ?? null;
                }
                else if (data is DateTimeOffset?)
                {
                    var dateTime = (DateTimeOffset?)data;
                    return dateTime?.ToString("s") ?? null;
                }
                else if (data is string)
                {
                    var dataStr = (string)data;
                    if (dataStr.Contains("'"))
                    {
                        dataStr = dataStr.Replace("'", "''");
                    }
                    return dataStr;
                }
                else
                {
                    return data?.ToString() ?? null;
                }
            }
            return null;
        }


        public string CreateInsertQueryFromParquetFile(DataColumn[] columns, List<Dictionary<string, string>> schema, string tableName)
        {
            string query = "";
            var shcemaFields = schema.Select(x => x.Keys.FirstOrDefault()).ToList(); 
            var schemaDataType = schema.Select(x => x.Values.FirstOrDefault()).ToList();

            for (int col = 0; col < columns[0].Data.Length; col++)
            {
                //var row = columns[r].Data;
                var insert = $"INSERT INTO \"{tableName}\" VALUES(";
                for (int row = 0; row < schema.Count; row++)
                {
                    var field = shcemaFields[row]; 
                    var dtCol = columns.FirstOrDefault(x => x.Field.Name.ToLower() == field.ToLower());
                    var val = GetValue(dtCol, col);
                    
                    if (schemaDataType[row] != null && (schemaDataType[row].ToLower() == "text" || schemaDataType[row].ToLower() == "datetime"))
                    {
                        insert += val != null ? $"'{val}'" : "null";
                    }
                    else
                    {
                        insert += val != null ? val : "null";
                    }

                    if(row < schema.Count - 1)
                        insert += ",";
                }
                query += insert + ");";
                //log_hash = GetValue(columns, 0, r);
                //time = GetValue(columns, 1, r);
                //modem_time = GetValue(columns, 2, r);
            }
            return query;
        }

        public string GetTableNameFromApacheFilePath(string filePath)
        {
            var t = filePath.Split("\\");
            var parquetFileName = t[t.Length - 1].Split(".")[0];
            var tableName = GetTableNameFromApacheFile(parquetFileName);

            return tableName;
        }

        private string GetTableNameFromApacheFile(string parquetFileName)
        {
            var data = parquetFileName.Split("_");
            string tableName = "";
            
            for (int i = 0; i < data.Length; i++)
            {
                var part = data[i];
                
                bool hasNoLetters = !part.Any(x => char.IsLetter(x));
                if (hasNoLetters)
                    break;

                tableName += part + "_";
            }
            tableName = tableName.Substring(0, tableName.Length - 1);
            return tableName;
        }

        public async Task<string> CreateMySQLInsertQueries(string filePath, Dictionary<string, List<Dictionary<string,string>>> schemas)
        {
            string insertQueries = "";

            var t = filePath.Split("\\");
            var parquetFileName = t[t.Length - 1].Split(".")[0];
            var tableName = GetTableNameFromApacheFile(parquetFileName); //parquetFileName.Split("_")[0].ToLower();
            var schema = schemas.GetValueOrDefault(tableName);

            Stream fileStream = File.OpenRead(filePath);
            ParquetReader parquetReader = await ParquetReader.CreateAsync(fileStream);
            DataField[] dataFields = parquetReader.Schema.GetDataFields();

            for (int i = 0; i < parquetReader.RowGroupCount; i++)
            {
                ParquetRowGroupReader groupReader = parquetReader.OpenRowGroupReader(i);
                var columns = new DataColumn[dataFields.Length];

                for (int c = 0; c < columns.Length; c++)
                {
                    columns[c] = await groupReader.ReadColumnAsync(dataFields[c]);
                }
                insertQueries += CreateInsertQueryFromParquetFile(columns, schema, tableName);
            }

            return insertQueries;
        }
    }
}
