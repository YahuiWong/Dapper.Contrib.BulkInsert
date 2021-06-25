using System;

namespace ClickHouse.Ado.Client
{
    public class AopEvents
    {
        public Action<ClientException> OnError;
        public Action<ClickHouseCommand> OnLogExecuting;
        public Action<ClickHouseCommand> OnLogExecuted;
    }
}
