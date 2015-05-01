using System;
using System.Collections.Generic;
using System.Linq;

namespace DataConverter
{
    public class Gesture
    {
        public List<Point> Points = new List<Point>();

        public DateTime Created;

        public DateTime Ended
        {
            get { return Created + Points.Last().T; }
        }
    }
}
