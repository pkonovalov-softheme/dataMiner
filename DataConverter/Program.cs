using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataConverter
{
    class Program
    {
        const string connectionString = @"Server = 127.0.0.1; Database = webData;persist security info=True; Integrated Security=SSPI;;MultipleActiveResultSets=true";

        static void Main(string[] args)
        {
            //var bulkCopy = new SqlBulkCopy(connection) { DestinationTableName = "session_events" };
            //bulkCopy.CommandText =
            //    "DELETE FROM dbo.BulkCopyDemoMatchingColumns";
            //bulkCopy.ExecuteNonQuery();

            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();

                using (connection)
                {
                    var proc = new DbProcessor(connection);
                    proc.CreateGuestures();
                }
            }

        }
    }
}
