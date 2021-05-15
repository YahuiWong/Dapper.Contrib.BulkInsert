using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using Dapper;

namespace Dapper.Contrib.BulkInsert
{
    public static partial class SqlMapperExtensions
    {
      

        /// <summary>
        /// Inserts an entity into table "Ts" and returns identity id or number of inserted rows if inserting a list.
        /// </summary>
        /// <param name="connection">Open SqlConnection</param>
        /// <param name="entityToInsert">Entity to insert, can be list of entities</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout</param>
        /// <returns>Identity of inserted entity, or number of inserted rows if inserting a list</returns>
        public static async Task InsertBulkAsync<T>(this IDbConnection connection, IEnumerable<T> entityToInsert, int? commandTimeout = null)
        {
            var cmd = GenerateBulkSql(connection, entityToInsert);

            var wasClosed = connection.State == ConnectionState.Closed;
            if (wasClosed) connection.Open();

            await connection.ExecuteAsync(cmd.Item1,cmd.Item2, null, commandTimeout);
            if (wasClosed) connection.Close();
        }
        /// <summary>
        /// Inserts an entity into table "Ts" and returns identity id or number of inserted rows if inserting a list.
        /// </summary>
        /// <param name="connection">Open SqlConnection</param>
        /// <param name="entityToInsert">Entity to insert, can be list of entities</param>
        /// <returns>Identity of inserted entity, or number of inserted rows if inserting a list</returns>
        public static async Task InsertBulkAsync<T>(this ClickHouseConnection connection, IEnumerable<T> entityToInsert, int? commandTimeout = null)
        {

            var wasClosed = connection.State == ConnectionState.Closed;
            if (wasClosed) connection.Open();


            var type = entityToInsert.GetType();
            var typeInfo = type.GetTypeInfo();
            bool implementsGenericIEnumerableOrIsGenericIEnumerable =
                typeInfo.ImplementedInterfaces.Any(ti =>
                    ti.IsGenericType() && ti.GetGenericTypeDefinition() == typeof(IEnumerable<>)) ||
                typeInfo.GetGenericTypeDefinition() == typeof(IEnumerable<>);

            if (implementsGenericIEnumerableOrIsGenericIEnumerable)
            {
                type = type.GetGenericArguments()[0];
            }

            var name = GetTableName(type);
            var sbColumnList = new StringBuilder(null);
            var allProperties = TypePropertiesCache(type);
            var keyProperties = KeyPropertiesCache(type);
            var computedProperties = ComputedPropertiesCache(type);
            var allPropertiesExceptKeyAndComputed =
                allProperties.Except(keyProperties.Union(computedProperties)).ToList();

            for (var i = 0; i < allPropertiesExceptKeyAndComputed.Count; i++)
            {
                var property = allPropertiesExceptKeyAndComputed[i];
                sbColumnList.AppendFormat("`{0}`", GetColumnName(property));
                if (i < allPropertiesExceptKeyAndComputed.Count - 1)
                    sbColumnList.Append(", ");
            }


            List<List<object>> rowList = new List<List<object>>();
            for (int j = 0, length = Enumerable.Count(entityToInsert); j < length; j++)
            {
                var item = Enumerable.ElementAt(entityToInsert, j);

                {
                    List<object> row = new List<object>();

                    for (var i = 0; i < allPropertiesExceptKeyAndComputed.Count; i++)
                    {
                        var property = allPropertiesExceptKeyAndComputed[i];

                        var val = property.GetValue(item);
                        if (property.PropertyType.IsValueType)
                        {
                            if (property.PropertyType == typeof(DateTime))
                            {
                                var datetimeValue = (DateTime)val;

                                if (property.GetCustomAttribute<DateAttribute>() != null)
                                {
                                    row.Add(datetimeValue.Date);
                                }
                                else
                                {
                                    row.Add(datetimeValue);
                                }
                            }
                            else
                            {
                                row.Add(val);
                            }
                        }
                        else
                        {
                            row.Add(val);
                        }
                    }
                    rowList.Add(row);
                }
            }
            using (var bulkCopy = new ClickHouseBulkCopy(connection)
            {
                DestinationTableName = name,
                MaxDegreeOfParallelism = 2,
                BatchSize = 10000
            })
            {
                var rows = rowList.Select(s => s.ToArray());
                await bulkCopy.WriteToServerAsync(rows, new[] { sbColumnList.ToString() });
            }



            if (wasClosed) connection.Close();
        }

    }


}
