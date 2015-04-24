using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace DataConverter
{
        public enum MouseEventTypes : byte 
        {  
           Unknown = 0,
           Scroll = 1,
           Mousemove = 2,   
           Click = 3,    
           DblClick = 4,   
           MouseDown = 5,   
           MouseUp = 6,   
           MouseOut = 7,  
           MouseOver = 8  
        }

        public struct MouseEvent
        {
            /// <summary>
            /// Event type id
            /// </summary>
            public MouseEventTypes EventType { get; private set; }

            /// <summary>
            /// Session start relative timestamp
            /// </summary>
            public TimeSpan SessionTimeStamp { get; private set; }

            /// <summary>
            /// X document coordinat system, pixel
            /// </summary>
            public Int16 X { get; private set; }

            /// <summary>
            /// X document coordinat system, pixel
            /// </summary>
            public Int16 Y { get; private set; }

            /// <summary>
            /// Page width, pixel
            /// </summary>
            public Int16 Width { get; private set; }

            /// <summary>
            /// Page height, pixel
            /// </summary>
            public Int16 Height { get; private set; }

            /// <summary>
            /// Url length
            /// </summary>
            public Int16 UrlLength { get; private set; }

            /// <summary>
            /// Url length
            /// </summary>
            public string Url { get; private set; }

            public bool IsValid
            {
                get
                {
                    if (X < 0 || Y < 0)
                    {
                        return false;
                    }

                    if (UrlLength < 0)
                    {
                        return false;
                    }

                    return true;
                }
            }

           public static MouseEvent ReadMouseEvent(BinaryReader reader)
           {
               var mevent = new MouseEvent();
               mevent.EventType = (MouseEventTypes)reader.ReadByte();
               mevent.SessionTimeStamp = TimeSpan.FromMilliseconds(reader.ReadInt32());
               mevent.X = reader.ReadInt16();
               mevent.Y = reader.ReadInt16();
               mevent.Width = reader.ReadInt16();
               mevent.Height = reader.ReadInt16();
               mevent.UrlLength = reader.ReadInt16();

               mevent.Url = Encoding.UTF8.GetString(reader.ReadBytes(mevent.UrlLength));
               return mevent;
           }
        }
}
