using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MathNet.Numerics;
using MathNet.Numerics.Interpolation;

namespace DataConverter
{
    static class GuestureCreator
    {
        private static readonly TimeSpan BreakTime = TimeSpan.FromMilliseconds(40);
        private const int BreakSpeed = 5; // px/sec
        private static double maxSpeed = 0;
        private static double maxSpeedTs = 0;

        public static List<Gesture> CreateGestures(List<MouseEvent> sessionEvents)
        {
            var result = new List<Gesture>();

            var gesture = new Gesture();

            sessionEvents = sessionEvents.OrderBy(sessionEvent => sessionEvent.EventDateTime).ToList();

            FixBadEvents(sessionEvents);

            for (int i = 1; i < sessionEvents.Count; i++)
            {
                MouseEvent curPoint = sessionEvents[i];
                MouseEvent prevPoint = sessionEvents[i - 1];

                double dist = Euclidean(curPoint, prevPoint);
                TimeSpan difTime = curPoint.SessionTimeStamp - prevPoint.SessionTimeStamp;

                double speed = dist / difTime.TotalSeconds;
                if (!double.IsInfinity(speed) && speed > maxSpeed)
                {
                    maxSpeed = speed;
                    maxSpeedTs = difTime.TotalMilliseconds;
                    if (maxSpeed > 200000)
                    {
                        Debugger.Break();
                    }
                }

                if (difTime > BreakTime || speed < BreakSpeed)
                {
                    if (gesture.Points.Count > 0)
                    {
                        result.Add(gesture);
                    }

                    gesture = new Gesture();
                }
                else
                {
                    Point point = new Point(curPoint.X, curPoint.Y, curPoint.SessionTimeStamp);

                    if (gesture.Points.Count == 0)
                    {
                        gesture.Created = curPoint.EventDateTime;
                    }

                    gesture.Points.Add(point);
                }

            }

            if (gesture.Points.Count > 0)
            {
                result.Add(gesture);
            }

            return result;
        }

        private static void FixBadEvents(List<MouseEvent> sessionEvents)
        {
            List<double> goodEventsX = new List<double>(sessionEvents.Count);
            List<double> goodEventsY= new List<double>(sessionEvents.Count);

            List<MouseEvent> badEvents = new List<MouseEvent>();

            List<double> goodTimes = new List<double>(sessionEvents.Count);
            MouseEvent last = null;
            MouseEvent prevEvent = null;
            DateTime firstDate;

            //if (sessionEvents.Count == 0)
            //{
            //    return;
            //}

            firstDate = sessionEvents.First().EventDateTime;

            for (int i = 0; i < sessionEvents.Count; i++)
            {
                var sessionEvent = sessionEvents[i];
                if (sessionEvent.EventDateTime < firstDate)
                {
                    Debugger.Break();
                }

                double sesSpan = (sessionEvent.EventDateTime - firstDate).TotalMilliseconds;

                if (sessionEvent.IsValid)
                {
                    if (!goodTimes.Contains(sesSpan))
                    {
                        goodEventsX.Add(sessionEvent.X);
                        goodEventsY.Add(sessionEvent.Y);
                        goodTimes.Add(sesSpan);
                        last = sessionEvent;
                    }
                    else
                    {
                        sessionEvents.RemoveAt(i);
                        i--;
                    }
                }
                else
                {
                    if (!goodTimes.Contains(sesSpan))
                    {
                        sessionEvent.PrevGood = last;
                        badEvents.Add(sessionEvent);
                        last = null;
                    }
                    else
                    {
                        sessionEvents.RemoveAt(i);
                        i--;
                    }
                }
            }

            IInterpolation interpX;
            IInterpolation interpY;

            if (goodTimes.Count > 5)
            {
                interpX = Interpolate.CubicSplineRobust(goodTimes, goodEventsX);
                interpY = Interpolate.CubicSplineRobust(goodTimes, goodEventsY);
            }
            else if (goodTimes.Count > 2)
            {
                interpX = Interpolate.Linear(goodTimes, goodEventsX);
                interpY = Interpolate.Linear(goodTimes, goodEventsY);
            }
            else
            {
                foreach (var badEvent in badEvents)
                {
                    badEvent.X = 0;
                    badEvent.Y = 0;
                }  
 
                return;
            }

            for (int i = 0; i < badEvents.Count; i++)
            {
                var badEvent = badEvents[i];
                double sesSpan = (badEvent.EventDateTime - firstDate).TotalMilliseconds;

                double intX = interpX.Interpolate(sesSpan);
                double intY = interpY.Interpolate(sesSpan);

                Debug.Assert(!double.IsInfinity(intX));
                Debug.Assert(!double.IsInfinity(intY));

                if (intX < 0)
                {
                    intX = 0;
                }

                if (intY < 0)
                {
                    intY = 0;
                }

                badEvent.X = (short)intX;
                badEvent.Y = (short)intY;
               

                Debug.Assert(badEvent.X <= 100000);
                Debug.Assert(badEvent.Y <= 100000);

            }
        }

        /// <summary>
        /// Return the Euclidean distance between 2 points
        /// </summary>
        public static double Euclidean(MouseEvent p1, MouseEvent p2)
        {
            double dist = Distance.Euclidean(new double[] { p1.X, p1.Y }, new double[] { p2.X, p2.Y });
            return dist;
        }
    }
}
