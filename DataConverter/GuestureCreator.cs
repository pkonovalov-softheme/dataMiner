﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
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

        public static List<Gesture> CreateGestures(List<MouseEvent> chunkEvents, DateTime created)
        {
            var result = new List<Gesture>();

            var gesture = new Gesture();

           // chunkEvents = chunkEvents.OrderBy(sessionEvent => sessionEvent.EventDateTime).ToList();

            FixBadEvents(chunkEvents);

            for (int i = 1; i < chunkEvents.Count; i++)
            {
                MouseEvent curPoint = chunkEvents[i];
                MouseEvent prevPoint = chunkEvents[i - 1];

                double dist = Euclidean(curPoint, prevPoint);
                TimeSpan difTime = curPoint.T - prevPoint.T;

                double speed = dist / difTime.TotalSeconds;
                if (!double.IsInfinity(speed) && speed > maxSpeed)
                {
                    maxSpeed = speed;
                    maxSpeedTs = difTime.TotalMilliseconds;
                    if (maxSpeed > 200000)
                    {
                       // Debugger.Break();
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
                    Point point = new Point(curPoint.X, curPoint.Y, curPoint.T);

                    if (gesture.Points.Count == 0)
                    {
                        gesture.Created = created + point.T;
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

        private static void SaveEvents(List<MouseEvent> sessionEvents, int from, int to)
        {
            var formatter = new BinaryFormatter();
            List<MouseEvent> targetList = sessionEvents.GetRange(from, to - from);


            using (FileStream fs = new FileStream(@"C:\temp\1.txt", FileMode.Create))
            {
                formatter.Serialize(fs, targetList);
            }
        }

        public static List<MouseEvent> ResoreEvents()
        {
            var formatter = new BinaryFormatter();

            using (FileStream fs = new FileStream(@"C:\temp\1.txt", FileMode.Open))
            {
                return (List<MouseEvent>)formatter.Deserialize(fs);
            }
        }

        public static void FixBadEvents(List<MouseEvent> sessionEvents)
        {
            List<double> goodEventsX = new List<double>(sessionEvents.Count);
            List<double> goodEventsY= new List<double>(sessionEvents.Count);

            List<MouseEvent> badEvents = new List<MouseEvent>();

            List<double> goodTimes = new List<double>(sessionEvents.Count);
            MouseEvent last = null;
            MouseEvent prevEvent = null;
            //DateTime firstDate;

           // MouseEvent prevM = null;
            //firstDate = sessionEvents.First().EventDateTime;

            for (int i = 0; i < sessionEvents.Count; i++)
            {
                var sessionEvent = sessionEvents[i];
                //if (sessionEvent.EventType == MouseEventTypes.Scroll)
                //{
                //    if (!sessionEvent.IsValid)
                //        Debugger.Break();
                //}

                //if (i > 0 && sessionEvent.EventType == MouseEventTypes.Mousemove && sessionEvents[i - 1].EventType == MouseEventTypes.Scroll)
                //{
                //    Debugger.Break();
                //}

                if (sessionEvent.EventType != MouseEventTypes.Scroll)
                {
                    //if (!sessionEvent.IsValid)
                    //    Debugger.Break();
                    //MouseEvent good =
                    //    sessionEvents.GetRange(i, sessionEvents.Count - i)
                    //        .FirstOrDefault(ses => ses.EventType == MouseEventTypes.Mousemove);

                    //for (int k = i; k < sessionEvents.Count; k++)
                    //{

                    //}
                }

                if (sessionEvent.IsValid)
                {
                    if (!goodTimes.Contains(sessionEvent.T.TotalMilliseconds))
                    {
                        goodEventsX.Add(sessionEvent.X);
                        goodEventsY.Add(sessionEvent.Y);
                        goodTimes.Add(sessionEvent.T.TotalMilliseconds);
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
                    if (!goodTimes.Contains(sessionEvent.T.TotalMilliseconds))
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
                interpX = Interpolate.Linear(goodTimes, goodEventsX);
                interpY = Interpolate.Linear(goodTimes, goodEventsY);
            }
            else if (goodTimes.Count > 2)
            {
                interpX = Interpolate.Linear(goodTimes, goodEventsX);
                interpY = Interpolate.Linear(goodTimes, goodEventsY);
            }
            else if (goodTimes.Count > 0)
            {
                foreach (var badEvent in badEvents)
                {
                    badEvent.X = (short)goodEventsX.First();
                    badEvent.Y = (short)goodEventsY.First();
                }
  
                return;
            }
            else
            {
                //Debugger.Break();
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

                //if (badEvent.id == 6843 || badEvent.id == 6844 || badEvent.id == 7305)
                //    Debugger.Break();
                double intX = interpX.Interpolate(badEvent.T.TotalMilliseconds);
                double intY = interpY.Interpolate(badEvent.T.TotalMilliseconds);

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
