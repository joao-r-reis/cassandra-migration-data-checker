using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace DataChecker
{
    internal class ReportPipe
    {
        private long _countFailed;
        private long _countSuccess;
        private long _countErrored;
        private ActionBlock<CheckReport> _actionBlock;

        public ReportPipe()
        {
            _countFailed = 0;
            _countSuccess = 0;
            _countErrored = 0;
            var printStatusInterval = TimeSpan.FromSeconds(30);
            var lastPrint = DateTime.UtcNow;
            _actionBlock = new ActionBlock<CheckReport>(r =>
            {
                if (r.FailedRowChecks > 0 || r.ErroredRowChecks > 0)
                {
                    Trace.WriteLine(
                        "Found errors in latest report: " +
                            $"{r.FailedRowChecks} failed, {r.ErroredRowChecks} errored." +
                        $"{Environment.NewLine}Failed keys: {string.Join(", ", r.FailedKeys.Select(k => k.ToString()))}.");
                }

                Interlocked.Add(ref _countFailed, r.FailedRowChecks);
                Interlocked.Add(ref _countErrored, r.ErroredRowChecks);
                Interlocked.Add(ref _countSuccess, r.SuccessfulRowChecks);

                var now = DateTime.UtcNow;
                if (now - lastPrint >= printStatusInterval)
                {
                    Trace.WriteLine("Status: " +
                                      $"{Interlocked.Read(ref _countSuccess)} successful, " +
                                      $"{Interlocked.Read(ref _countFailed)} failed, " +
                                      $"{Interlocked.Read(ref _countErrored)} errored.");
                    lastPrint = now;
                }
            },
                new ExecutionDataflowBlockOptions
                {
                    BoundedCapacity = Program.TargetReadParallelism * Program.TargetReadBatch,
                    SingleProducerConstrained = false
                });
        }

        public ITargetBlock<CheckReport> Block => _actionBlock;

        public async Task WaitAndPrintReport()
        {
            await _actionBlock.Completion.ConfigureAwait(false);
            Trace.WriteLine(
                "Done! " +
                $"{Interlocked.Read(ref _countSuccess)} successful, " +
                $"{Interlocked.Read(ref _countFailed)} failed, " +
                $"{Interlocked.Read(ref _countErrored)} errored.");
        }
    }
}