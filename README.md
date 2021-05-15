# Dapper.Contrib.BulkInsert

Easy to use Dapper batch insert, support MySQL, SQLServer, ClickHouse


## Demo

### Entity

```c#
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
```
### InsertBulk
```c#
MySqlConnection conn = new MySqlConnection("server=127.0.0.1;Database=Demo;Uid=root;Pwd=root");
conn.Open();
conn.Execute("CREATE TABLE IF NOT EXISTS TestUser (ResisterDate Date, ResisterTime DateTime, Name varchar(200), Age int(11)) ENGINE=InnoDB");
var user = new TestUser() { ResisterDate = DateTime.Now, ResisterTime = DateTime.Now, Age = 18, Name = "Jack" };
var user2 = new TestUser() { ResisterDate = DateTime.Now, ResisterTime = DateTime.Now, Age = 18, Name = "Tom" };

var users = new List<TestUser>() { user, user2 };
conn.InsertBulk(users);
```



Dapper: https://github.com/StackExchange/Dapper

ClickHouse: https://github.com/yandex/ClickHouse

ClickHouse Ado.NET Driver: https://github.com/YahuiWong/ClickHouse.Client
