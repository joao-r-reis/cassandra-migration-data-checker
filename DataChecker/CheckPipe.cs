using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Cassandra;

namespace DataChecker
{
    class CheckPipe
    {
        private BatchBlock<Row> _batchBlock;
        private TransformBlock<Row[], CheckReport> _transformBlock;

        public CheckPipe(ISession session, ITargetBlock<CheckReport> nextBlock)
        {
            _batchBlock = new BatchBlock<Row>(Program.TargetReadBatch, new GroupingDataflowBlockOptions
            {
                EnsureOrdered = false,
                BoundedCapacity = Program.TargetReadBatch*Program.TargetReadParallelism,
            });

            var ps = session.Prepare(
                "select keyname, value, ttl(value) as value_ttl from baselines.keyvalue WHERE keyname = ?").SetIdempotence(true);

            _transformBlock = new TransformBlock<Row[], CheckReport>(async rows =>
            {
                var tasks = new Task<RowSet>[rows.Length];
                for (var i = 0; i < rows.Length; i++)
                {
                    var stmt = ps.Bind(rows[i]["keyname"]);
                    tasks[i] = session.ExecuteAsync(stmt);
                }

                try
                {
                    await Task.WhenAll(tasks).ConfigureAwait(false);
                }
                catch
                {
                }

                var countSuccess = 0;
                var countFail = 0;
                var countError = 0;
                var failedKeys = new List<object>();
                for (var i = 0; i < rows.Length; i++)
                {
                    var targetRowTask = tasks[i];
                    try
                    {
                        var targetRow = (await targetRowTask.ConfigureAwait(false)).SingleOrDefault();

                        if (object.Equals(rows[i]["keyname"], targetRow["keyname"]) &&
                            object.Equals(rows[i]["value"], targetRow["value"]) &&
                            object.Equals(rows[i]["value_ttl"], targetRow["value_ttl"]))
                        {
                            countSuccess++;
                        }
                        else
                        {
                            failedKeys.Add(rows[i]["keyname"]);
                            countFail++;
                        }
                    }
                    catch (Exception ex)
                    {
                        failedKeys.Add(rows[i]["keyname"]);
                        countError++;
                        Trace.WriteLine("Error in CheckPipe: " + ex.ToString());
                        continue;
                    }
                }

                return new CheckReport
                {
                    FailedKeys = failedKeys,
                    FailedRowChecks = countFail,
                    SuccessfulRowChecks = countSuccess,
                    ErroredRowChecks = countError
                };
            }, new ExecutionDataflowBlockOptions
            {
                BoundedCapacity = Program.TargetReadParallelism,
                EnsureOrdered = false, 
                SingleProducerConstrained = true, 
                MaxDegreeOfParallelism = Program.TargetReadParallelism
            });

            _batchBlock.LinkTo(_transformBlock, new DataflowLinkOptions { PropagateCompletion = true});
            _transformBlock.LinkTo(nextBlock, new DataflowLinkOptions { PropagateCompletion = true});
        }

        public BatchBlock<Row> Block => _batchBlock;
    }
}
