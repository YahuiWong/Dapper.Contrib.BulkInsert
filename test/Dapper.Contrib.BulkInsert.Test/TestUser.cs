using System;
using System.Collections.Generic;
using System.Text;

namespace Dapper.Contrib.BulkInsert.Test
{
    [Table("TestUser")]
    public class TestUser
    {
        [Date]
        public DateTime ResisterDate { get; set; }
        public DateTime ResisterTime { get; set; }
        public string Name { get; set; }
        public int Age { get; set; }
    }
}
