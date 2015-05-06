using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MathNet;
using MathNet.Numerics;
using MathNet.Numerics.Interpolation;


namespace DataConverter
{
    internal class Program
    {
        private const string connectionString =
            @"Server = 127.0.0.1; Database = webData;persist security info=True; Integrated Security=SSPI;;MultipleActiveResultSets=true";

        private static void Main(string[] args)
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

                    //MouseEvent p2 = new MouseEvent();

                    //double[] _x = { 1, 5, 9 };
                    //double[] _t = { 1, 2, 3 };

                    //double[] times = new[] {12759.0, 12767.0, 14324.0, 14431.0, 14482.0, 21952.0};

                    //double[]  vals = new[] { 1027.0, 1007.0, 975.0, 1041.0, 1044.0, 732.0 };

                    //IInterpolation interp = Interpolate.RationalWithPoles(times, vals);
                    //double res = interp.Interpolate(20952.0);

                    List<MouseEvent> sessionEvents = GuestureCreator.ResoreEvents();


                    double maxSpeed = 0;
                    double maxSpeedTs = 0;

                    sessionEvents = sessionEvents.OrderBy(sessionEvent => sessionEvent.EventDateTime).ToList();

                    DateTime firstEventDate = sessionEvents.First().EventDateTime;
                    foreach (MouseEvent me in sessionEvents)
                    {
                        me.SessionTimeStamp = me.EventDateTime - firstEventDate;
                    }

                    GuestureCreator.FixBadEvents(sessionEvents);

                    for (int i = 1; i < sessionEvents.Count; i++)
                    {
                        MouseEvent curPoint = sessionEvents[i];
                        MouseEvent prevPoint = sessionEvents[i - 1];

                        double dist = GuestureCreator.Euclidean(curPoint, prevPoint);
                        TimeSpan difTime = curPoint.EventDateTime - prevPoint.EventDateTime;

                        double speed = dist/difTime.TotalSeconds;
                        if (!double.IsInfinity(speed) && speed > maxSpeed)
                        {
                            maxSpeed = speed;
                            maxSpeedTs = difTime.TotalMilliseconds;
                            if (maxSpeed > 200000)
                            {
                                Debugger.Break();
                            }
                        }
                    }
                }

            }
        }
    }
}

