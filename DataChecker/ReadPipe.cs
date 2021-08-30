using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

using Cassandra;

namespace DataChecker
{
    internal class ReadPipe
    {
        private ActionBlock<Tuple<long, long>> actionBlock;
        private CancellationTokenSource _cts = new CancellationTokenSource();

        public ReadPipe(ISession session, BatchBlock<Row> nextPipe, BufferBlock<Tuple<long, long>> buffer)
        {
            actionBlock = new ActionBlock<Tuple<long, long>>(async tuple =>
            {
                var stmt = (IStatement)new SimpleStatement(
                    $"select keyname, value, ttl(value) as value_ttl from baselines.keyvalue WHERE token(keyname) >= {tuple.Item1} AND token(keyname) <= {tuple.Item2}");
                stmt = stmt.SetPageSize(100000).SetIdempotence(true);
                try
                {
                    var rs = await session.ExecuteAsync(stmt).ConfigureAwait(false);
                    do
                    {
                        var rows = rs.Take(rs.GetAvailableWithoutFetching());
                        foreach (var r in rows)
                        {
                            if (!await nextPipe.SendAsync(r, _cts.Token).ConfigureAwait(false))
                            {
                                if (_cts.Token.IsCancellationRequested)
                                {
                                    return;
                                }

                                throw new InvalidOperationException("Could not send rows to next pipe.");
                            }
                        }

                        Trace.WriteLine($"Finished reading for tokens [{tuple.Item1},{tuple.Item2}]");

                        await rs.FetchMoreResultsAsync().ConfigureAwait(false);
                    } while (!_cts.IsCancellationRequested && rs.GetAvailableWithoutFetching() > 0);
                }
                catch (Exception ex)
                {
                    Trace.WriteLine("FAILURE: " + ex.ToString());
                }

            },
                new ExecutionDataflowBlockOptions
                {
                    MaxDegreeOfParallelism = Program.OriginReadParallelism,
                    BoundedCapacity = Program.OriginReadParallelism,
                    EnsureOrdered = false
                });
            actionBlock.Completion.ContinueWith(_ =>
            {
                nextPipe.TriggerBatch();
                nextPipe.Complete();
            });
        }

        public ActionBlock<Tuple<long, long>> Block => actionBlock;
    }
}