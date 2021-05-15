
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using Xunit;

namespace Dapper.Contrib.BulkInsert.Test
{
    public class MySQLUnitTest
    {
        MySqlConnection conn;
        public MySQLUnitTest()
        {
            conn = new MySqlConnection("server=127.0.0.1;Database=Demo;Uid=root;Pwd=root");
            conn.Open();
            CreateTable(conn);
        }
        private void CreateTable(MySqlConnection conn)
        {
            conn.Execute(
                "CREATE TABLE IF NOT EXISTS TestUser (ResisterDate Date, ResisterTime DateTime, Name varchar(200), Age int(11)) ENGINE=InnoDB");
            
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
