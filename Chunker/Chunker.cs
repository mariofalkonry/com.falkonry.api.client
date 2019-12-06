using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;

namespace falkonry.com.util
{
    public enum ChunkSize { _8MB = 8, 
                            _16MB =16,
                            _24MB = 24,
                            _32MB = 32,
                            _48MB = 48,
                            _64MB = 64,
                            _128MB = 128}
    public class Chunker
    {
        // Very risky as it may not line up, encodings use variable byte sizes
        public static void ChunkTextFromBytes(ChunkSize size,Stream instrm,Action<String> callback,Encoding encoding=null)
        {
            if (encoding == null)
                encoding = Encoding.UTF8;
            if (!instrm.CanRead)
                throw (new InvalidOperationException("Instrm is not readable"));
            int chunkMB= ((int)size) * 1024 * 1024;
            using (Stream buf = new BufferedStream(instrm, chunkMB))
            {
                byte[] readData = new byte[chunkMB];
                do
                {
                    buf.Read(readData, 0, readData.Length);
                    if (readData.Length > 0)
                        callback(encoding.GetString(readData));
                }
                while (readData.Length > 0);
            }
        }

        public static void ChunkBytesFromBytes(ChunkSize size, Stream instrm, Action<Byte[]> callback)
        {
            if (!instrm.CanRead)
                throw (new InvalidOperationException("Instrm is not readable"));
            int chunkMB = ((int)size) * 1024 * 1024;
            using (Stream buf = new BufferedStream(instrm, chunkMB))
            {
                byte[] readData = new byte[chunkMB];
                do
                {
                    buf.Read(readData, 0, readData.Length);
                    if (readData.Length > 0)
                        callback(readData);
                }
                while (readData.Length > 0);
            }
        }

        public static void ChunkTextFromText(ChunkSize size, TextReader rdr, Action<String> callback, Encoding encoding = null,bool hasHeader=true)
        {
            if (encoding == null)
                encoding = Encoding.UTF8;
            int chunkMB = ((int)size) * 1024 * 1024;
            int chunkSize = 0;
            bool firstLine = true;
            String header = null;
            StringBuilder toSend = new StringBuilder();
            do
            {
                var line = rdr.ReadLine();
                if (line == null)
                    break;
                if(firstLine && hasHeader)
                {
                    header = line;
                    firstLine = false;
                    toSend.AppendLine(line); /// Don't count on size to send so that a header is always included
                    continue;
                }
                toSend.AppendLine(line);
                chunkSize += encoding.GetBytes(line).Length;
                if (chunkSize >= chunkMB) 
                {
                    callback(toSend.ToString());
                    toSend.Clear();
                    if(hasHeader)
                        toSend.AppendLine(header);
                    chunkSize = 0;
                }
            }
            while (true);
            if (toSend.Length > 0)
                callback(toSend.ToString()); // Send remaining lines
        }

        public static void ChunkBytesFromText(ChunkSize size, TextReader rdr, Action<Byte[]> callback, Encoding encoding = null)
        {
            if (encoding == null)
                encoding = Encoding.UTF8;
            int chunkMB = ((int)size) * 1024 * 1024;
            int chunkSize = 0;
            List<Byte> toSend = new List<byte>();
            do
            {
                var line = rdr.ReadLine();
                if (line == null)
                    break;
                var bytes = encoding.GetBytes(line);
                toSend.AddRange(bytes);
                toSend.AddRange(encoding.GetBytes(Environment.NewLine));

                chunkSize += bytes.Length;
                if (chunkSize >= chunkMB)
                {
                    callback(toSend.ToArray());
                    toSend.Clear();
                    chunkSize = 0;
                }
            }
            while (true);
            if (toSend.Count > 0)
                callback(toSend.ToArray()); // Send remaining lines
        }

        public static ChunkSize GetChunkSize(uint mbs)
        {
            if (mbs <= (int)ChunkSize._8MB)
                return ChunkSize._8MB;
            else if (mbs <= (int)ChunkSize._16MB)
                return ChunkSize._16MB;
            else if (mbs <= (int)ChunkSize._24MB)
                return ChunkSize._24MB;
            else if (mbs <= (int)ChunkSize._24MB)
                return ChunkSize._24MB;
            else if (mbs <= (int)ChunkSize._32MB)
                return ChunkSize._32MB;
            else if (mbs <= (int)ChunkSize._48MB)
                return ChunkSize._48MB;
            else if (mbs <= (int)ChunkSize._64MB)
                return ChunkSize._64MB;
            else
                return ChunkSize._128MB;
        }
    }
}
