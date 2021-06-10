using ClickHouse.Ado;
using System;
using System.Collections.Generic;
using Xunit;

namespace Dapper.Contrib.BulkInsert.Test
{
    public class ClickHouseADOUnitTest
    {
        ClickHouseConnection conn;
        public ClickHouseADOUnitTest()
        {
            conn = new ClickHouseConnection("Compress=True;CheckCompressedHash=False;Compressor=lz4;Host=127.0.0.1;Port=9000;Database=default;User=default;Password=;");
            conn.Open();
            CreateTable(conn);
        }
        private void CreateTable(ClickHouseConnection conn)
        {
            conn.Execute(
                "CREATE TABLE IF NOT EXISTS TestUser (ResisterDate Date, ResisterTime DateTime, Name String, Age Int32) ENGINE=MergeTree(ResisterDate,(ResisterTime,Name,Age), 8192)");
            
        }
        [Fact]
        public void TestInsertBulk()
        {
            var user = new TestUser() { ResisterDate = DateTime.Now, ResisterTime = DateTime.Now, Age = 18, Name = "张三23" };
            var user2 = new TestUser() { ResisterDate = DateTime.Now, ResisterTime = DateTime.Now, Age = 18, Name = "张三3" };

            var users = new List<TestUser>() { user, user2 };
            conn.InsertBulk(users);
        }

    }
}
