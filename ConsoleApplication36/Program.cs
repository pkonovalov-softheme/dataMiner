using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.Remoting.Messaging;
using System.Runtime.Remoting.Metadata.W3cXsd2001;
using System.Text;
using System.Threading.Tasks;
using LumenWorks.Framework.IO.Csv;

namespace DataImporter
{
    class Program
    {

        private const string TargetFile = @"I:\data\session_events7\session_events7.csv";
        private static readonly Stopwatch Watch = new Stopwatch();
        private static int _totalLinesCount;
        public static byte[] GetStringToBytes(string value)
        {
            SoapHexBinary shb = SoapHexBinary.Parse(value);
            return shb.Value;
        }

        static void Main(string[] args)
        {
            Console.WriteLine("Reading file..");
            _totalLinesCount = File.ReadLines(TargetFile).Count();
            //_totalLinesCount = 4547395;

            Watch.Start();

            const string connectionString = @"Server = 127.0.0.1; Database = webData; User Id = 1;Password = 123asdQ!;";
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                ReadCsvLineByLine(connection);
            }
        }

        public static byte[] StringToByteArray(String hex)
        {
            int NumberChars = hex.Length;
            byte[] bytes = new byte[NumberChars / 2];
            for (int i = 2; i < NumberChars; i += 2)
                bytes[i / 2] = Convert.ToByte(hex.Substring(i, 2), 16);
            return bytes;
        }

        static void ReadCsvLineByLine(SqlConnection connection)
        {
            const int bulkCount = 1000;
            int curRecordInBulk = 0;
            ulong curRowInTable = 0;

            DataTable sessionEvents = CreateSessionEventsTable();

            var bulkCopy = new SqlBulkCopy(connection) { DestinationTableName = sessionEvents.TableName };

            using (var csv =
                   new CsvReader(new StreamReader(TargetFile), true, '\t'))
            {
                while (csv.ReadNextRecord())
                {
                    try
                    {
                        DataRow workRow = sessionEvents.NewRow();
                        
                        for (int i = 0; i < csv.FieldCount; i++)
                        {
                            if (string.IsNullOrEmpty(csv[i]))
                            {
                                workRow[i] = DBNull.Value;
                            }
                            else if (sessionEvents.Columns[i].DataType == typeof(Guid))
                            {
                                string guid = csv[i];

                                if (string.IsNullOrEmpty(guid))
                                {
                                    workRow[i] = DBNull.Value;
                                }
                                else
                                {
                                    workRow[i] = Guid.Parse(guid);
                                }
                            }
                            else if (sessionEvents.Columns[i].DataType == typeof(byte[]))
                            {
                                string ministring = csv[i].Substring(2);

                                if (!string.IsNullOrEmpty(ministring))
                                {
                                    byte[] bytes = GetStringToBytes(ministring);
                                    Debug.Assert((ushort) bytes[0] <= 8);
                                    workRow[i] = bytes;
                                }
                            }
                            else if (sessionEvents.Columns[i].DataType == typeof(Int32))
                            {
                                workRow[i] = Int32.Parse(csv[i]);
                            }
                            else
                            {
                                workRow[i] = csv[i];
                            }
                        }

                        sessionEvents.Rows.Add(workRow);

                        curRecordInBulk++;
                        curRowInTable++;

                        if (curRecordInBulk == bulkCount)
                        {
                            double proc = (double)curRowInTable / _totalLinesCount;
                            Console.WriteLine(" {0} % done", (int)(proc * 100));


                            var remainingMsecs = (ulong)(Watch.ElapsedMilliseconds / proc);
                            Console.WriteLine(" Time in way minutes: {0}, left minutes: {1} ", Watch.Elapsed.Minutes, TimeSpan.FromMilliseconds(remainingMsecs).Minutes);

                            bulkCopy.BulkCopyTimeout = 5000;
                            bulkCopy.WriteToServer(sessionEvents);
                            curRecordInBulk = 0;
                            sessionEvents.Rows.Clear();
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                        Debugger.Break();
                    }
 
                }
            }
        }

        private static DataTable CreateSessionEventsTable()
        {
            var orderTable = new DataTable("session_events");

            //var colId = new DataColumn("key", typeof(UInt64));
            //orderTable.Columns.Add(colId);

            // Define one column.
            var colId = new DataColumn("visitor_id", typeof(Guid));
            orderTable.Columns.Add(colId);

            colId = new DataColumn("created", typeof(Int64));
            orderTable.Columns.Add(colId);

            colId = new DataColumn("category_id", typeof(String));
            orderTable.Columns.Add(colId);

            colId = new DataColumn("collector_ver", typeof(String));
            orderTable.Columns.Add(colId);

            colId = new DataColumn("discount", typeof(String));
            orderTable.Columns.Add(colId);

            colId = new DataColumn("event_id", typeof(Guid));
            orderTable.Columns.Add(colId);

            colId = new DataColumn("event_name", typeof(String));
            orderTable.Columns.Add(colId);

            colId = new DataColumn("item_id", typeof(String));
            orderTable.Columns.Add(colId);

            colId = new DataColumn("items", typeof(String));
            orderTable.Columns.Add(colId);

            colId = new DataColumn("page_type", typeof(String));
            orderTable.Columns.Add(colId);

            colId = new DataColumn("page_hash", typeof(String));
            orderTable.Columns.Add(colId);

            colId = new DataColumn("page_url", typeof(String));
            orderTable.Columns.Add(colId);

            colId = new DataColumn("page_visit_id", typeof(Guid));
            orderTable.Columns.Add(colId);

            colId = new DataColumn("payment_method", typeof(String));
            orderTable.Columns.Add(colId);

            colId = new DataColumn("qnts", typeof(String));
            orderTable.Columns.Add(colId);

            colId = new DataColumn("recommender_element_id", typeof(Int32));
            orderTable.Columns.Add(colId);

            colId = new DataColumn("recommender_id", typeof(Guid));
            orderTable.Columns.Add(colId);

            colId = new DataColumn("session_id", typeof(Guid));
            orderTable.Columns.Add(colId);

            colId = new DataColumn("shipping_method", typeof(String));
            orderTable.Columns.Add(colId);

            colId = new DataColumn("shipping_price", typeof(String));
            orderTable.Columns.Add(colId);

            colId = new DataColumn("site_id", typeof(Int32));
            orderTable.Columns.Add(colId);

            colId = new DataColumn("stream_data_chunk", typeof(byte[]));
            orderTable.Columns.Add(colId);

            colId = new DataColumn("total", typeof(String));
            orderTable.Columns.Add(colId);

            colId = new DataColumn("total_inc_tax", typeof(String));
            orderTable.Columns.Add(colId);

            colId = new DataColumn("tracker_ver", typeof(String));
            orderTable.Columns.Add(colId);

            return orderTable;
        }
    }


}
