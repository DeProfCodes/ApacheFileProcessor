using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Data_Analytics_Tools.Helpers
{
    public class SQL
    {
        SqlConnection Connection;
        SqlCommand command;

        public enum SqlExecutionType
        {
            Scalar,
            NonQuery
        };

        struct TempData
        {
            public string databaseName;
            public string tableName;
            public int totalInsertRows;

            public void Set(string tableName, int totalInsertRows)
            {
                this.tableName = tableName;
                this.totalInsertRows = totalInsertRows;
            }
        };

        TempData tempData;

        bool testMode = false;

        const string INSUFFICIENT_MEMORY = "Insufficient Memory";
        const string INSPECT_SCHEMA = "Insepct Schema";

        public SQL()
        {
            string connectionString = "Server=SYNERGY-7U24F9O\\GCWENSASERVER;Database=apacheLogsToMySqlMemory;User Id=gcwensaUser;Password=Gcwensa123;Trusted_Connection=True;MultipleActiveResultSets=true";
            Connection = new SqlConnection(connectionString);
        }

        public async Task CreateDatabase(string databaseName)
        {
            if (testMode)
                databaseName += "_TEST";

            // DO NOT DROP TABLE, rather resume the import process
            /*   
            string query = $"DROP DATABASE {databaseName}";
            RunQuery(query);
            */
            string query = $"CREATE DATABASE {databaseName}";
            await RunQuery(query);

            string connectionString = $"Server=SYNERGY-7U24F9O\\GCWENSASERVER;Database={databaseName};User Id=gcwensaUser;Password=Gcwensa123;Trusted_Connection=True;";
            Connection = new SqlConnection(connectionString);

            tempData.databaseName = databaseName;
        }

        public async Task<int> RunBulkQueries(List<string> queries)
        {
            int rows = 0;
            foreach (string query in queries)
            {
                rows += await RunQuery(query);
            }
            return rows;
        }

        public async Task<int> ClearTable(string tableName)
        {
            string query = $"DELETE FROM {tableName}";
            int rows = await RunQuery(query);

            return rows;
        }

        public int InsertIntoTable(string tableName, string query, List<string> insertQueries)
        {
            int rows = 0;
            try
            {

            }
            catch (Exception ex)
            {
                if (ex.Message == INSUFFICIENT_MEMORY)
                {

                }
            }
            return rows;
        }

        public async Task<bool> RowExist(string tableName, string insertQuery)
        {
            if (!insertQuery.ToLower().Contains("insert"))
                return true;

            string[] values = insertQuery.Split("(");
            string[] values2 = values[1].Split(",");
            string log_hash = values2[0];
            string time = values2[1];

            string count = $"SELECT COUNT(*) FROM {tableName} WHERE log_hash = {log_hash} AND time={time}";
            int rows = await RunQuery(count, SqlExecutionType.Scalar);

            return rows > 0;
        }

        public async Task<int> RunQuery(string query, SqlExecutionType executionType = SqlExecutionType.NonQuery)
        {
            int rows = 0;
            object rowsObj = null;

            try
            {
                command = new SqlCommand(query, Connection);
                command.CommandTimeout = 3600;

                Connection.Close();
                Connection.Open();

                if (executionType == SqlExecutionType.NonQuery)
                {
                    rows = command.ExecuteNonQuery();
                }
                else if (executionType == SqlExecutionType.Scalar)
                {
                    rowsObj = command.ExecuteScalar();
                }
                Connection.Close();
            }
            catch (Exception ex)
            {
                if (ex.Message.Contains("There is already an object named"))
                {
                    //all good: DROP TABLE and CREATE TABLE
                }
                else if (ex.Message.Contains("Database") && ex.Message.Contains("already exists"))
                {
                    //all good
                }
                else if (ex.Message.Contains("There is insufficient system memory in resource pool"))
                {
                    throw new Exception(INSUFFICIENT_MEMORY);
                }
                else if (ex.Message.Contains("Operand type clash: numeric is incompatible with text"))
                {
                    throw new Exception(INSPECT_SCHEMA);
                }
                else
                {
                    string msg = ex.Message;
                }
            }

            if (executionType == SqlExecutionType.NonQuery)
            {
                return rows;
            }
            else if (executionType == SqlExecutionType.Scalar)
            {
                if (rowsObj != null && rowsObj is int)
                {
                    return (int)rowsObj;
                }
            }
            return -1;
        }
    }
}
