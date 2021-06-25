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
        [ClickHouseColumn(Name= "ResisterTime", IsOnlyIgnoreInsert =true)]
        public DateTime ResisterTime { get; set; }
        [ColumnName("Name")]
        public string Name { get; set; }
        public int Age { get; set; }
    }
}
