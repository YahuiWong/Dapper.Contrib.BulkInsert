using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace ClickHouse.Ado.Client
{
    public class ClientException: Exception
    {
        public new Exception InnerException;
        public new string StackTrace;
        public new MethodBase TargetSite;
        public new string Source;
        public ClickHouseCommand command;


        public ClientException(string message)
            : base(message) { }

        public ClientException(string message, ClickHouseCommand command)
            : base(message)
        {
            this.command = command;
        }
        public ClientException(Exception ex, ClickHouseCommand command)
            : base(ex.Message)
        {
            this.InnerException = ex.InnerException;
            this.StackTrace = ex.StackTrace;
            this.TargetSite = ex.TargetSite;
            this.Source = ex.Source;
            this.command = command;
        }
    }
}
