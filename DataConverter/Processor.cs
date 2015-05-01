using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Runtime.Serialization.Formatters.Binary;

namespace DataConverter
{
    class DbProcessor
    {
        private readonly SqlConnection _connection;
        private readonly HashSet<Guid> _badSessionsId = new HashSet<Guid>(); 

        public DbProcessor(SqlConnection connection)
        {
            _connection = connection;
        }

        private static DataTable CreateGuesturesTable()
        {
            var orderTable = new DataTable("guestures");

            var colId = new DataColumn("guesture_id", typeof(Guid));
            orderTable.Columns.Add(colId);

            colId = new DataColumn("type", typeof(string));
            orderTable.Columns.Add(colId);

            colId = new DataColumn("session_id", typeof(Guid));
            orderTable.Columns.Add(colId);

            colId = new DataColumn("created", typeof(DateTime));
            orderTable.Columns.Add(colId);

            colId = new DataColumn("ended", typeof(DateTime));
            orderTable.Columns.Add(colId);

            colId = new DataColumn("points", typeof(byte[]));
            orderTable.Columns.Add(colId);

            return orderTable;
        }

        private static DataTable CreateSesionStatsTable()
        {
            var orderTable = new DataTable("sessions_stats");

            var colId = new DataColumn("session_id", typeof(Guid));
            orderTable.Columns.Add(colId);

            colId = new DataColumn("CorrectEventsCount", typeof(Int32));
            orderTable.Columns.Add(colId);

            colId = new DataColumn("InvalidEventsCount", typeof(Int32));
            orderTable.Columns.Add(colId);

            colId = new DataColumn("Rate", typeof(double));
            orderTable.Columns.Add(colId);

            return orderTable;
        }

        private static DataTable CreateSerialEventsTable()
        {
            var orderTable = new DataTable("serial_events2");

            var colId = new DataColumn("session_id", typeof(Guid));
            orderTable.Columns.Add(colId);

            colId = new DataColumn("created", typeof(DateTime));
            orderTable.Columns.Add(colId);

            colId = new DataColumn("site_id", typeof(Int32));
            orderTable.Columns.Add(colId);

            colId = new DataColumn("event_name", typeof(String));
            orderTable.Columns.Add(colId);

            colId = new DataColumn("page_url", typeof(String));
            orderTable.Columns.Add(colId);

            colId = new DataColumn("stream_data_chunk", typeof(byte[]));
            orderTable.Columns.Add(colId);

            colId = new DataColumn("event_id", typeof(Guid));
            orderTable.Columns.Add(colId);

            return orderTable;
        }

        public void CreateRawEventsBulk()
        {
            const int bulkSize = 1;
            int curRecordInBulk = 0;

            var watch = Stopwatch.StartNew();
            ulong curRow = 0;

            //var commandCount = new SqlCommand(
            //         "SELECT Count(*) FROM [webdata].[dbo].[session_events] where [event_name] = 'MouseEvents' or [event_name] = 'OrderPurchased'" +
            //         " or [event_name] = 'PageClose'",
            //         _connection);

            //ulong totalRowsCount = Convert.ToUInt64(commandCount.ExecuteScalar());

            ulong totalRowsCount = 100000;

            //var buffer = new byte[1024];
            Guid currentSessionId = Guid.Empty;

            DataTable serialEvents = CreateSerialEventsTable();

            var bulkCopy = new SqlBulkCopy(@"Server = 127.0.0.1; Database = webData;persist security info=True; Integrated Security=SSPI;;", SqlBulkCopyOptions.KeepIdentity) { DestinationTableName = serialEvents.TableName };
            bulkCopy.BatchSize = 50;

            var command = new SqlCommand(
                     "SELECT [created], [event_name], [page_url],[page_visit_id], [session_id], [site_id], [stream_data_chunk], [event_id]" +
                     "FROM [webdata].[dbo].[session_events] where [event_name] = 'MouseEvents' or [event_name] = 'OrderPurchased'" +
                     " or [event_name] = 'PageClose' order by [session_id], [created] ",
                     _connection);

            using (SqlDataReader sessionEventsReader = command.ExecuteReader())
            {
                while (sessionEventsReader.Read())
                {
                    DataRow workRow = serialEvents.NewRow();

                    int pageVisitIdColumnIndex = sessionEventsReader.GetOrdinal("session_id");
                    Guid sessionId = sessionEventsReader.GetGuid(pageVisitIdColumnIndex);

                    int siteIdIndex = sessionEventsReader.GetOrdinal("site_id");
                    int siteId = sessionEventsReader.GetInt32(siteIdIndex);

                    int createdColumnIndex = sessionEventsReader.GetOrdinal("created");
                    long created = sessionEventsReader.GetInt64(createdColumnIndex);

                    int pageUrlIndex = sessionEventsReader.GetOrdinal("page_url");
                    string pageUrl = sessionEventsReader.GetString(pageUrlIndex);

                    int eventIdColumnIndex = sessionEventsReader.GetOrdinal("event_id");
                    Guid eventId = sessionEventsReader.GetGuid(eventIdColumnIndex);

                    int eventNameIndex = sessionEventsReader.GetOrdinal("event_name");
                    string eventName = sessionEventsReader.GetString(eventNameIndex);

                    // int streamDataChunkIndex = sessionEventsReader.GetOrdinal("event_name");

                    byte[] streamDataChunk;
                    if (sessionEventsReader["stream_data_chunk"] == DBNull.Value)
                    {
                        streamDataChunk = null;
                    }
                    else
                    {
                        streamDataChunk = (byte[])sessionEventsReader["stream_data_chunk"];//sessionEventsReader.GetBytes(streamDataChunkIndex, buffer, 1, buffer.Length,);
                    }

                    //if (currentSessionId != sessionId)
                    //{
                    //    Tuple<DateTime, string> sesStart = GetPageSessionStart(sessionId, created);

                    //    if (sesStart == null)
                    //    {
                    //        sesStart = new Tuple<DateTime, string>(UnixTimeStampToDateTime((ulong)created), pageUrl);
                    //    }

                    //    Debug.Assert(sesStart.Item1 <= UnixTimeStampToDateTime((ulong)created));

                    //    workRow["session_id"] = sessionId;
                    //    workRow["created"] = sesStart.Item1;
                    //    workRow["page_url"] = sesStart.Item2;
                    //    workRow["site_id"] = siteId;
                    //    workRow["event_name"] = "session_started";
                    //    workRow["event_id"] = Guid.NewGuid();

                    //    currentSessionId = sessionId;
                    //}

                    workRow["session_id"] = sessionId;
                    workRow["created"] = UnixTimeStampToDateTime((ulong)created);
                    workRow["page_url"] = pageUrl;
                    workRow["site_id"] = siteId;
                    workRow["event_name"] = eventName;
                    workRow["event_id"] = eventId;

                    if (streamDataChunk == null)
                    {
                        workRow["stream_data_chunk"] = DBNull.Value;
                    }
                    else
                    {
                        workRow["stream_data_chunk"] = streamDataChunk;
                    }

                    serialEvents.Rows.Add(workRow);
                    curRow++;
                    curRecordInBulk++;

                    if (curRecordInBulk == bulkSize)
                    {
                        bulkCopy.WriteToServer(serialEvents);
                        curRecordInBulk = 0;
                        double proc = (double)curRow / totalRowsCount;
                        Console.WriteLine(" {0} % done", (int)(proc * 100));
                        var remainingMsecs = (ulong)(watch.ElapsedMilliseconds / proc);
                        Console.WriteLine(" Time in way minutes: {0}, left minutes: {1} ", watch.Elapsed.Minutes, TimeSpan.FromMilliseconds(remainingMsecs).Minutes);
                    }
                }
            }
        }

        public void CreateRawEvents()
        {
            int writePeriod = 3000;
            int rowInPeriod = 0;

            var watch = Stopwatch.StartNew();
            ulong curRow = 0;

            var commandCount = new SqlCommand(
                     "SELECT Count(*) FROM [webdata].[dbo].[session_events] where [event_name] = 'MouseEvents' or [event_name] = 'OrderPurchased'" +
                     " or [event_name] = 'PageClose'",
                     _connection);

            ulong totalRowsCount = Convert.ToUInt64(commandCount.ExecuteScalar());

            SqlCommand insertCommand;
            byte[] streamDataChunk;

            //var buffer = new byte[1024];
            Guid currentSessionId = Guid.Empty;

            var command = new SqlCommand(
                     "SELECT [created], [event_name], [page_url],[page_visit_id], [session_id], [site_id], [stream_data_chunk], [event_id]" +
                     "FROM [webdata].[dbo].[session_events] where [event_name] = 'MouseEvents' or [event_name] = 'OrderPurchased'" +
                     " or [event_name] = 'PageClose' order by [session_id], [created] ",
                     _connection);

            using (SqlDataReader sessionEventsReader = command.ExecuteReader())
            {
                while (sessionEventsReader.Read())
                {
                    int pageVisitIdColumnIndex = sessionEventsReader.GetOrdinal("session_id");
                    Guid sessionId = sessionEventsReader.GetGuid(pageVisitIdColumnIndex);

                    int siteIdIndex = sessionEventsReader.GetOrdinal("site_id");
                    int siteId = sessionEventsReader.GetInt32(siteIdIndex);

                    int createdColumnIndex = sessionEventsReader.GetOrdinal("created");
                    long created = sessionEventsReader.GetInt64(createdColumnIndex);

                    int pageUrlIndex = sessionEventsReader.GetOrdinal("page_url");
                    string pageUrl = sessionEventsReader.GetString(pageUrlIndex);

                    int eventIdColumnIndex = sessionEventsReader.GetOrdinal("event_id");
                    Guid eventId = sessionEventsReader.GetGuid(eventIdColumnIndex);

                    int eventNameIndex = sessionEventsReader.GetOrdinal("event_name");
                    string eventName = sessionEventsReader.GetString(eventNameIndex);

                   // int streamDataChunkIndex = sessionEventsReader.GetOrdinal("event_name");

                    if (sessionEventsReader["stream_data_chunk"] == DBNull.Value)
                    {
                        streamDataChunk = null;
                    }
                    else
                    {
                        streamDataChunk = (byte[])sessionEventsReader["stream_data_chunk"];//sessionEventsReader.GetBytes(streamDataChunkIndex, buffer, 1, buffer.Length,);
                    }
          
                    if (currentSessionId != sessionId)
                    {
                        Tuple<DateTime, string> sesStart = GetPageSessionStart(sessionId, created);

                        if (sesStart == null)
                        {
                            sesStart = new Tuple<DateTime, string>(UnixTimeStampToDateTime((ulong)created), pageUrl);
                        }

                        Debug.Assert(sesStart.Item1 <= UnixTimeStampToDateTime((ulong)created));
                        insertCommand = new SqlCommand(
                            "INSERT INTO [webdata].[dbo].[serial_events2] ([key], [session_id], [created], [site_id], [event_name], [page_url], [event_id]) " +
                            "VALUES(@key, @session_id, @created, @site_id, @event_name, @page_url, @event_id)", _connection);

                        insertCommand.Parameters.Add("@key", SqlDbType.BigInt).Value = curRow;
                        insertCommand.Parameters.Add("@session_id", SqlDbType.UniqueIdentifier).Value = sessionId;
                        insertCommand.Parameters.Add("@created", SqlDbType.DateTime).Value = sesStart.Item1;
                        insertCommand.Parameters.Add("@page_url", SqlDbType.VarChar).Value = sesStart.Item2;
                        insertCommand.Parameters.Add("@site_id", SqlDbType.Int).Value = siteId;
                        insertCommand.Parameters.Add("@event_name", SqlDbType.VarChar).Value = "session_started";
                        insertCommand.Parameters.Add("@event_id", SqlDbType.UniqueIdentifier).Value = Guid.NewGuid();
                        insertCommand.ExecuteNonQuery();

                        currentSessionId = sessionId;

                        rowInPeriod++;
                        curRow++;
                    }

                    insertCommand = new SqlCommand(
                        "INSERT INTO [webdata].[dbo].[serial_events2] ([key], [session_id], [created], [site_id], [event_name], [page_url], [stream_data_chunk], [event_id]) " +
                        "VALUES(@key, @session_id, @created, @site_id, @event_name, @page_url, @stream_data_chunk, @event_id)", _connection);

                    insertCommand.Parameters.Add("@key", SqlDbType.BigInt).Value = curRow;
                    insertCommand.Parameters.Add("@session_id", SqlDbType.UniqueIdentifier).Value = sessionId;
                    insertCommand.Parameters.Add("@created", SqlDbType.DateTime).Value = UnixTimeStampToDateTime((ulong)created);
                    insertCommand.Parameters.Add("@site_id", SqlDbType.Int).Value = siteId;
                    insertCommand.Parameters.Add("@page_url", SqlDbType.VarChar).Value = pageUrl;

                    if (streamDataChunk == null)
                    {
                        insertCommand.Parameters.Add("@stream_data_chunk", SqlDbType.VarBinary).Value = DBNull.Value;
                    }
                    else
                    {
                        insertCommand.Parameters.Add("@stream_data_chunk", SqlDbType.VarBinary).Value = streamDataChunk;
                    }
 
                    insertCommand.Parameters.Add("@event_name", SqlDbType.VarChar).Value = eventName;
                    insertCommand.Parameters.Add("@event_id", SqlDbType.UniqueIdentifier).Value = eventId;

                    insertCommand.ExecuteNonQuery();

                    if (rowInPeriod >= writePeriod)
                    {
                        double proc = (double) curRow/totalRowsCount;
                        Console.WriteLine(" {0} % done", (int) (proc*100));
                        var remainingMsecs = (ulong) (watch.ElapsedMilliseconds/proc);
                        Console.WriteLine(" Time in way minutes: {0}, left minutes: {1} ", watch.Elapsed.Minutes,
                            TimeSpan.FromMilliseconds(remainingMsecs).TotalMinutes);

                        rowInPeriod = 0;
                    }

                    rowInPeriod++;
                    curRow++;
                }
            }
        }

        private static DateTime UnixTimeStampToDateTime(ulong unixTimeStamp)
        {
            // Unix timestamp is seconds past epoch
            var dtDateTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
            dtDateTime = dtDateTime.AddMilliseconds(unixTimeStamp).ToLocalTime();

            Debug.Assert(dtDateTime >  new DateTime(1990, 1, 1));
            Debug.Assert(dtDateTime < new DateTime(2030, 1, 1));

            return dtDateTime;
        }

        private Tuple<DateTime, string> GetPageSessionStart(Guid pageSessionId, long minCreatedDate)
        {
            string cmd =
                string.Format(
                    "SELECT TOP 1 [created], [this_url] FROM [webdata].[dbo].[page_visits] where [session_id] = '{0}' and [created] <= '{1}'  order by created DESC",
                    pageSessionId, minCreatedDate);

            var command = new SqlCommand(cmd, _connection);

            using (SqlDataReader reader = command.ExecuteReader())
            {
                if (!reader.HasRows)
                {
                    return null;
                }

                reader.Read();
                ulong unixTimeStamp = Convert.ToUInt64(reader["created"]);
                DateTime time = UnixTimeStampToDateTime(unixTimeStamp);

                return new Tuple<DateTime, string>(time, reader["this_url"].ToString());
            }
        }


        private List<MouseEvent> ReadMousePositionEvents(byte[] streamDataChunk, Guid sessionId)
        {
            List<MouseEvent> result = new List<MouseEvent>();
            var stream = new MemoryStream(streamDataChunk);
            var reader = new BinaryReader(stream);

            do
            {
                var currentEvent = MouseEvent.ReadMouseEvent(reader);

                if (currentEvent.EventType == MouseEventTypes.Mousemove ||
                    currentEvent.EventType == MouseEventTypes.Scroll)
                {
                    result.Add(currentEvent);
                    //if (currentEvent.SessionTimeStamp.TotalMilliseconds == 1480.0)
                    //{
                    //    Debugger.Break();
                    //}
                }
            } while (stream.Position != stream.Length);


            return result;
        }

        public void CreateStats()
        {
            ulong rowPeriod = 1000;
            List<MouseEvent> sessionMouseEvents = new List<MouseEvent>();
            Guid prevSesId = Guid.Empty;
            ulong curRow = 0;
            var watch = Stopwatch.StartNew();

            DataTable sessionsStats = CreateSesionStatsTable();
            var bulkCopy = new SqlBulkCopy(@"Server = 127.0.0.1; Database = webData;persist security info=True; Integrated Security=SSPI;;", SqlBulkCopyOptions.KeepIdentity) { DestinationTableName = sessionsStats.TableName };


            var commandCount = new SqlCommand(
                     "SELECT Count(*) FROM [webdata].[dbo].[serial_events]",
                     _connection);

            ulong totalRowsCount = Convert.ToUInt64(commandCount.ExecuteScalar());


            var serialCommand = new SqlCommand(
                             "SELECT [event_id], [session_id], [created], [event_name], [stream_data_chunk] FROM [webdata].[dbo].[serial_events] where session_id = '58e2d4b9-81ab-4918-b0f1-0010a7596f26'",
                            _connection);

             using (SqlDataReader sessionEventsReader = serialCommand.ExecuteReader())
            {
                while (sessionEventsReader.Read())
                {
                    int pageVisitIdColumnIndex = sessionEventsReader.GetOrdinal("session_id");
                    Guid sessionId = sessionEventsReader.GetGuid(pageVisitIdColumnIndex);

                    int createdColumnIndex = sessionEventsReader.GetOrdinal("created");
                    DateTime created = sessionEventsReader.GetDateTime(createdColumnIndex);

                    int eventNameIndex = sessionEventsReader.GetOrdinal("event_name");
                    string eventName = sessionEventsReader.GetString(eventNameIndex);

                    int eventIdIndex = sessionEventsReader.GetOrdinal("event_id");
                    Guid eventId = sessionEventsReader.GetGuid(eventIdIndex);


                    if (prevSesId != Guid.Empty && prevSesId != sessionId)
                    {

                        int validMouseEventsCount = sessionMouseEvents.Count(curEvent => curEvent.IsValid);
                        double validRate = (double)validMouseEventsCount / sessionMouseEvents.Count;

                        DataRow curStatRow = sessionsStats.NewRow();
                        curStatRow["session_id"] = prevSesId;
                        curStatRow["CorrectEventsCount"] = validMouseEventsCount;
                        curStatRow["InvalidEventsCount"] = sessionMouseEvents.Count - validMouseEventsCount;

                        if (double.IsNaN(validRate))
                        {
                            curStatRow["Rate"] = DBNull.Value;
                        }
                        else
                        {
                            curStatRow["Rate"] = validRate;
                        }

                        sessionsStats.Rows.Add(curStatRow);
                        sessionMouseEvents = new List<MouseEvent>();
                    }

                    prevSesId = sessionId;

                    byte[] streamDataChunk;
                    if (sessionEventsReader["stream_data_chunk"] == DBNull.Value)
                    {
                        streamDataChunk = null;
                    }
                    else
                    {
                        streamDataChunk = (byte[])sessionEventsReader["stream_data_chunk"];
                        List<MouseEvent> events = ReadMousePositionEvents(streamDataChunk, sessionId);
                        sessionMouseEvents.AddRange(events);
                    }

                    curRow++;

                    if (curRow%rowPeriod == 0)
                    {
                        double proc = (double) curRow/totalRowsCount;
                        Console.WriteLine(" {0} % done", (int) (proc*100));
                        var remainingMsecs = (ulong) (watch.ElapsedMilliseconds/proc);
                        Console.WriteLine(" Time in way minutes: {0}, left minutes: {1} ", watch.Elapsed.Minutes, TimeSpan.FromMilliseconds(remainingMsecs).Minutes);
                    }
                    
                }
            }

            bulkCopy.WriteToServer(sessionsStats);
            
        }

        public void UpdateTime()
        {
            ulong rowPeriod = 1000;
            List<MouseEvent> sessionMouseEvents = new List<MouseEvent>();
            Guid prevSesId = Guid.Empty;
            ulong curRow = 0;
            var watch = Stopwatch.StartNew();
            DateTime sessionStarted = DateTime.MinValue;


            var commandCount = new SqlCommand(
                     "SELECT Count(*) FROM [webdata].[dbo].[serial_events] where [event_name] != 'session_started' and event_name != 'PageClose' and [pageSessionOffset] is null",
                     _connection);

            ulong totalRowsCount = Convert.ToUInt64(commandCount.ExecuteScalar());


            var serialCommand = new SqlCommand(
                             "SELECT [event_id] FROM [webdata].[dbo].[serial_events] where [event_name] != 'session_started' and event_name != 'PageClose' and [pageSessionOffset] is null", 
                            _connection);

            using (SqlDataReader serialEventsReader = serialCommand.ExecuteReader())
            {
                while (serialEventsReader.Read())
                {
                        int eventIdIndex = serialEventsReader.GetOrdinal("event_id");
                        Guid eventId = serialEventsReader.GetGuid(eventIdIndex);

                        var sesEventsCommand = new SqlCommand(
                            "SELECT [page_visit_id] FROM [webdata].[dbo].[session_events] where [event_id] = @event_id",
                            _connection);

                        sesEventsCommand.Parameters.AddWithValue("@event_id", eventId);

                        object pageVisitIdObj = sesEventsCommand.ExecuteScalar();
                        if (pageVisitIdObj == null)
                        {
                            continue;
                        }

                        Guid pageVisitId = Guid.Parse(pageVisitIdObj.ToString());

                        var visitsEventsCommand = new SqlCommand(
                            "SELECT [created] FROM [webdata].[dbo].[page_visits] where [this_page_visit_id] = @page_visit_id",
                            _connection);

                        visitsEventsCommand.Parameters.AddWithValue("@page_visit_id", pageVisitId);

                        object created = visitsEventsCommand.ExecuteScalar();

                        if (created == null)
                        {
                             continue;
                        }

                        ulong createdStamp = Convert.ToUInt64(created);

                        DateTime createdDate = UnixTimeStampToDateTime(createdStamp);

                        var updateSerialCommand = new SqlCommand(
                                 "UPDATE [webdata].[dbo].[serial_events] SET [pageSessionOffset] = @created where [event_id] = @event_id",
                                _connection);

                        updateSerialCommand.Parameters.AddWithValue("@event_id", eventId);
                        updateSerialCommand.Parameters.AddWithValue("@created", createdDate);
                        updateSerialCommand.ExecuteNonQuery();

                        curRow++;

                    if (curRow % rowPeriod == 0)
                    {
                        double proc = (double)curRow / totalRowsCount;
                        Console.WriteLine(" {0} % done", (int)(proc * 100));
                        var remainingMsecs = (ulong)(watch.ElapsedMilliseconds / proc);
                        Console.WriteLine(" Time in way minutes: {0}, left minutes: {1} ", watch.Elapsed.Minutes, TimeSpan.FromMilliseconds(remainingMsecs).Minutes);
                    }

                }
            }


            //bulkCopy.WriteToServer(guesturesTable);

        }

        public void CreateGuestures()
        {
            ulong rowPeriod = 1000;
            List<MouseEvent> sessionMouseEvents = new List<MouseEvent>();
            Guid prevSesId = Guid.Empty;
            ulong curRow = 0;
            var watch = Stopwatch.StartNew();
            DateTime sessionStarted = DateTime.MinValue;
            BinaryFormatter formatter = new BinaryFormatter();

            DataTable guesturesTable = CreateGuesturesTable();
            var bulkCopy = new SqlBulkCopy(@"Server = 127.0.0.1; Database = webData;persist security info=True; Integrated Security=SSPI;;", SqlBulkCopyOptions.KeepIdentity) { DestinationTableName = guesturesTable.TableName };
            bulkCopy.BulkCopyTimeout = 200;

            var commandCount = new SqlCommand(
                     "SELECT Count(*) FROM [webdata].[dbo].[serial_events]",
                     _connection);

            ulong totalRowsCount = Convert.ToUInt64(commandCount.ExecuteScalar());


            var serialCommand = new SqlCommand(
                             "SELECT [event_id], [session_id], [created], [event_name], [stream_data_chunk] FROM [webdata].[dbo].[serial_events] where session_id = '1db52ace-207c-43c3-8bb1-0005ec491c57' ",
                            _connection);

            using (SqlDataReader sessionEventsReader = serialCommand.ExecuteReader())
            {
                while (sessionEventsReader.Read())
                {
                    int pageVisitIdColumnIndex = sessionEventsReader.GetOrdinal("session_id");
                    Guid sessionId = sessionEventsReader.GetGuid(pageVisitIdColumnIndex);

                    //if (sessionId == Guid.Parse("ba901ff3-9020-49a8-a3d5-00017867f874"))
                    //{
                    //    Debugger.Break(); 5e751bc0-6e30-4ca0-9719-000688be487f 
                    //}


                    int createdColumnIndex = sessionEventsReader.GetOrdinal("created");
                    DateTime created = sessionEventsReader.GetDateTime(createdColumnIndex);

                    int pageSessionOffsetColumnIndex = sessionEventsReader.GetOrdinal("created");
                    DateTime pageSessionOffset = sessionEventsReader.GetDateTime(pageSessionOffsetColumnIndex);

                    if (sessionStarted == DateTime.MinValue)
                    {
                        sessionStarted = created;
                    }

                    int eventNameIndex = sessionEventsReader.GetOrdinal("event_name");
                    string eventName = sessionEventsReader.GetString(eventNameIndex);

                    if (eventName == "session_started" || eventName == "PageClose" || eventName == "orderPurchased")
                    {
                        DataRow row = guesturesTable.NewRow();
                        row["guesture_id"] = Guid.NewGuid();
                        row["type"] = eventName;
                        row["session_id"] = sessionId;
                        row["created"] = created;
                        row["ended"] = created;

                        guesturesTable.Rows.Add(row);
                    }

                    int eventIdIndex = sessionEventsReader.GetOrdinal("event_id");
                    Guid eventId = sessionEventsReader.GetGuid(eventIdIndex);


                    if (prevSesId != Guid.Empty && prevSesId != sessionId)
                    {
                        if (sessionMouseEvents.Count > 0)
                        {
                            List<Gesture> guestures = GuestureCreator.CreateGestures(sessionMouseEvents);

                            foreach (var guesture in guestures)
                            {
                                DataRow row = guesturesTable.NewRow();
                                row["guesture_id"] = Guid.NewGuid();
                                row["type"] = "Gesture";
                                row["session_id"] = sessionId;
                                row["created"] = created;
                                row["ended"] = guesture.Ended;

                                MemoryStream memStream = new MemoryStream();
                                foreach (Point point in guesture.Points)
                                {
                                    formatter.Serialize(memStream, point);
                                }
 
                                byte[] data = memStream.ToArray();

                                row["points"] = data;

                                guesturesTable.Rows.Add(row);
                            }
                            
                            sessionMouseEvents = new List<MouseEvent>();
                            sessionStarted = created;
                        }
                    }

                    prevSesId = sessionId;

                    byte[] streamDataChunk;
                    if (sessionEventsReader["stream_data_chunk"] == DBNull.Value)
                    {
                        streamDataChunk = null;
                    }
                    else
                    {
                        streamDataChunk = (byte[])sessionEventsReader["stream_data_chunk"];
                        List<MouseEvent> events = ReadMousePositionEvents(streamDataChunk, sessionId);
                        events.ForEach(eventX => eventX.EventDateTime = pageSessionOffset + eventX.SessionTimeStamp);
                        sessionMouseEvents.AddRange(events);
                    }

                    curRow++;

                    if (curRow % rowPeriod == 0)
                    {
                        double proc = (double)curRow / totalRowsCount;
                        Console.WriteLine(" {0} % done", (int)(proc * 100));
                        var remainingMsecs = (ulong)(watch.ElapsedMilliseconds / proc);
                        Console.WriteLine(" Time in way minutes: {0}, left minutes: {1} ", watch.Elapsed.Minutes, TimeSpan.FromMilliseconds(remainingMsecs).Minutes);

                        //bulkCopy.WriteToServer(guesturesTable);
                        guesturesTable.Rows.Clear();
                    }

                }
            }

            List<Gesture> guestures2 = GuestureCreator.CreateGestures(sessionMouseEvents);

            

        }

    }
}
