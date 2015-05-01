using System;

namespace DataConverter
{
    [Serializable]
    public struct Point
    {
        public readonly int X;

        public readonly int Y;

        public readonly TimeSpan T;

        public Point(int x, int y, TimeSpan t)
        {
            X = x;
            Y = y;
            T = t;
        }

    }
}
