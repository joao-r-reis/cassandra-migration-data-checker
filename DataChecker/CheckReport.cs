using System;
using System.Collections.Generic;
using System.Text;

namespace DataChecker
{
    class CheckReport
    {
        public int ErroredRowChecks { get; set; }

        public int FailedRowChecks { get; set; }

        public int SuccessfulRowChecks { get; set; }

        public IEnumerable<object> FailedKeys { get; set; }
    }
}
