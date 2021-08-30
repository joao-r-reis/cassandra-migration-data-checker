using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Cassandra;

namespace DataChecker
{
    class Program
    {
        public const int OriginReadParallelism = 4;
        public const int TargetReadParallelism = 32;
        public static int TargetReadBatch = 512;
        public static int Ranges = 10000;
        public const int Connections = 32;

        /// <summary>
        /// Command line arguments are:
        ///
        /// <code>ORIGIN_CONTACT_POINT ORIGIN_USERNAME ORIGIN_PASSWORD TARGET_ASTRA_BUNDLE TARGET_USERNAME TARGET_PASSWORD TOKEN_RANGES TARGET_READ_BATCH </code>
        /// </summary>
        static void Main(string[] args)
        {
            MainAsync(args).GetAwaiter().GetResult();
        }
        
        static async Task MainAsync(string[] args)
        {
            Cassandra.Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Info;
            Trace.AutoFlush = true;
            Trace.Listeners.Add(new TextWriterTraceListener(Console.Out));
            Trace.Listeners.Add(new TextWriterTraceListener(File.Open("out.log", FileMode.Append, FileAccess.Write, FileShare.Read)));

            if (args.Length == 8)
            {
                Program.Ranges = int.Parse(args[6]);
                Program.TargetReadBatch = int.Parse(args[7]);
            }

            var origin = Cluster.Builder()
                .WithSocketOptions(new SocketOptions().SetStreamMode(true))
                .WithQueryOptions(new QueryOptions().SetConsistencyLevel(ConsistencyLevel.LocalQuorum))
                .AddContactPoint(args[0]).WithPoolingOptions(new PoolingOptions().SetCoreConnectionsPerHost(HostDistance.Local, Program.Connections))
                .WithCredentials(args[1], args[2])
                .Build().Connect();
            var target = Cluster.Builder()
                .WithSocketOptions(new SocketOptions().SetStreamMode(true))
                .WithQueryOptions(new QueryOptions().SetConsistencyLevel(ConsistencyLevel.LocalQuorum))
                .WithCloudSecureConnectionBundle(args[3])
                .WithPoolingOptions(new PoolingOptions().SetCoreConnectionsPerHost(HostDistance.Local, Program.Connections))
                .WithCredentials(args[4], args[5])
                .Build().Connect();

            Trace.WriteLine("Sessions connected.");

            var reportPipe = new ReportPipe();
            var checkPipe = new CheckPipe(target, reportPipe.Block);
            long min = (long)Math.Pow(-2, 63);
            long max = ((long)Math.Pow(2, 63)) - 1;
            var tokenCount = 18446744073709551615;

            var current = min;
            var count = (long) (tokenCount / (ulong)Program.Ranges);
            var tokenBlock = new BufferBlock<Tuple<long, long>>(new DataflowBlockOptions
                { BoundedCapacity = Program.OriginReadParallelism, EnsureOrdered = false });
            var readPipe = new ReadPipe(origin, checkPipe.Block, tokenBlock);
            tokenBlock.LinkTo(readPipe.Block, new DataflowLinkOptions { PropagateCompletion = true });

            Trace.WriteLine("Started.");
            
            while (current <= max)
            {
                var maxRange = current + count;
                if (maxRange > max || maxRange <= current)
                {
                    maxRange = max;
                }

                if (!await tokenBlock.SendAsync(new Tuple<long, long>(current, maxRange)).ConfigureAwait(false))
                {
                    throw new InvalidOperationException("could not send tokens.");
                }

                current = maxRange + 1;
                if (current >= max || current == min)
                {
                    break;
                }
            }
            tokenBlock.Complete();

            await reportPipe.WaitAndPrintReport().ConfigureAwait(false);
        }
    }
}
