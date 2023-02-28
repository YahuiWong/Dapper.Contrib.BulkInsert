using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Collections.Concurrent;


namespace Dapper.Contrib.BulkInsert
{
    /// <summary>
    /// The Dapper.Contrib extensions for Dapper
    /// </summary>
    public static partial class SqlMapperExtensions
    {
        /// <summary>
        /// Defined a proxy object with a possibly dirty state.
        /// </summary>
        public interface IProxy //must be kept public
        {
            /// <summary>
            /// Whether the object has been changed.
            /// </summary>
            bool IsDirty { get; set; }
        }

        /// <summary>
        /// Defines a table name mapper for getting table names from types.
        /// </summary>
        public interface ITableNameMapper
        {
            /// <summary>
            /// Gets a table name from a given <see cref="Type"/>.
            /// </summary>
            /// <param name="type">The <see cref="Type"/> to get a name from.</param>
            /// <returns>The table name for the given <paramref name="type"/>.</returns>
            string GetTableName(Type type);
        }

        /// <summary>
        /// The function to get a database type from the given <see cref="IDbConnection"/>.
        /// </summary>
        /// <param name="connection">The connection to get a database type name from.</param>
        public delegate string GetDatabaseTypeDelegate(IDbConnection connection);

        /// <summary>
        /// The function to get a a table name from a given <see cref="Type"/>
        /// </summary>
        /// <param name="type">The <see cref="Type"/> to get a table name for.</param>
        public delegate string TableNameMapperDelegate(Type type);

        /// <summary>
        /// The function to get a a column name from a given <see cref="Type"/>
        /// </summary>
        /// <param name="propertyInfo">The <see cref="PropertyInfo"/> to get a column name for.</param>
        public delegate string ColumnNameMapperDelegate(PropertyInfo propertyInfo);
        /// <summary>
        /// The function to get a a column name from a given <see cref="Type"/>
        /// </summary>
        /// <param name="propertyInfo">The <see cref="PropertyInfo"/> to get a column name for.</param>
        public delegate ClickHouseColumnAttribute ClickHouseColumnMapperDelegate(PropertyInfo propertyInfo);
        
        private static readonly ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>> KeyProperties =
            new ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>>();

        private static readonly ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>> ExplicitKeyProperties
            = new ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>>();

        private static readonly ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>> TypeProperties =
            new ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>>();

        private static readonly ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>> ComputedProperties =
            new ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>>();

        private static readonly ConcurrentDictionary<RuntimeTypeHandle, string> GetQueries =
            new ConcurrentDictionary<RuntimeTypeHandle, string>();

        private static readonly ConcurrentDictionary<RuntimeTypeHandle, string> TypeTableName =
            new ConcurrentDictionary<RuntimeTypeHandle, string>();
        private static readonly ConcurrentDictionary<int, string> TypeColumnName =
            new ConcurrentDictionary<int, string>();
        private static readonly ConcurrentDictionary<int, ClickHouseColumnAttribute> TypeClickHouseColumn =
          new ConcurrentDictionary<int, ClickHouseColumnAttribute>();
        private static List<PropertyInfo> ComputedPropertiesCache(Type type)
        {
            if (ComputedProperties.TryGetValue(type.TypeHandle, out IEnumerable<PropertyInfo> pi))
            {
                return pi.ToList();
            }

            var computedProperties = TypePropertiesCache(type)
                .Where(p => p.GetCustomAttributes(true).Any(a => a is ComputedAttribute)).ToList();

            ComputedProperties[type.TypeHandle] = computedProperties;
            return computedProperties;
        }

        private static List<PropertyInfo> ExplicitKeyPropertiesCache(Type type)
        {
            if (ExplicitKeyProperties.TryGetValue(type.TypeHandle, out IEnumerable<PropertyInfo> pi))
            {
                return pi.ToList();
            }

            var explicitKeyProperties = TypePropertiesCache(type)
                .Where(p => p.GetCustomAttributes(true).Any(a => a is ExplicitKeyAttribute)).ToList();

            ExplicitKeyProperties[type.TypeHandle] = explicitKeyProperties;
            return explicitKeyProperties;
        }

        private static List<PropertyInfo> KeyPropertiesCache(Type type)
        {
            if (KeyProperties.TryGetValue(type.TypeHandle, out IEnumerable<PropertyInfo> pi))
            {
                return pi.ToList();
            }

            var allProperties = TypePropertiesCache(type);
            var keyProperties = allProperties.Where(p => p.GetCustomAttributes(true).Any(a => a is KeyAttribute))
                .ToList();

            if (keyProperties.Count == 0)
            {
                var idProp = allProperties.Find(p =>
                    string.Equals(p.Name, "id", StringComparison.CurrentCultureIgnoreCase));
                if (idProp != null && !idProp.GetCustomAttributes(true).Any(a => a is ExplicitKeyAttribute))
                {
                    keyProperties.Add(idProp);
                }
            }

            KeyProperties[type.TypeHandle] = keyProperties;
            return keyProperties;
        }

        private static List<PropertyInfo> TypePropertiesCache(Type type)
        {
            if (TypeProperties.TryGetValue(type.TypeHandle, out IEnumerable<PropertyInfo> pis))
            {
                return pis.ToList();
            }

            var properties = type.GetProperties().Where(IsWriteable).ToArray();
            TypeProperties[type.TypeHandle] = properties;
            return properties.ToList();
        }

        private static bool IsWriteable(PropertyInfo pi)
        {
            var attributes = pi.GetCustomAttributes(typeof(WriteAttribute), false).AsList();
            if (attributes.Count != 1) return true;

            var writeAttribute = (WriteAttribute)attributes[0];
            return writeAttribute.Write;
        }


        /// <summary>
        /// Specify a custom table name mapper based on the POCO type name
        /// </summary>
        public static TableNameMapperDelegate TableNameMapper;

        private static string GetTableName(Type type)
        {
            if (TypeTableName.TryGetValue(type.TypeHandle, out string name)) return name;

            if (TableNameMapper != null)
            {
                name = TableNameMapper(type);
            }
            else
            {
                var info = type;
                //NOTE: This as dynamic trick falls back to handle both our own Table-attribute as well as the one in EntityFramework 
                var tableAttrName =
                    info.GetCustomAttribute<TableAttribute>(false)?.Name
                    ?? (info.GetCustomAttributes(false)
                        .FirstOrDefault(attr => attr.GetType().Name == "TableAttribute") as dynamic)?.Name;

                if (tableAttrName != null)
                {
                    name = tableAttrName;
                }
                else
                {
                    name = type.Name + "s";
                    if (type.IsInterface() && name.StartsWith("I"))
                        name = name.Substring(1);
                }
            }

            TypeTableName[type.TypeHandle] = name;
            return name;
        }
        /// <summary>
        /// Specify a custom Column name mapper based on the POCO type name
        /// </summary>
        public static ColumnNameMapperDelegate ColumnNameMapper;
        private static string GetColumnName(PropertyInfo propertyInfo)
        {
            if (TypeColumnName.TryGetValue(propertyInfo.GetHashCode(), out string name)) return name;
            if (ColumnNameMapper != null)
            {
                name = ColumnNameMapper(propertyInfo);
            }
            else
            {
                var info = propertyInfo;
                //NOTE: This as dynamic trick falls back to handle both our own Column-attribute as well as the one in EntityFramework 
                var columnAttrName =
                    info.GetCustomAttribute<ColumnNameAttribute>(false)?.Name
                    ?? (info.GetCustomAttributes(false)
                        .FirstOrDefault(attr => attr.GetType().Name == "ColumnNameAttribute") as dynamic)?.Name;

                if (columnAttrName != null)
                {
                    name = columnAttrName;
                }
                else
                {
                    var clickhouseColumn = GetClickHouseColumn(propertyInfo);

                    if (!string.IsNullOrEmpty(clickhouseColumn?.Name))
                        name = clickhouseColumn.Name;
                    else
                        name = info.Name;

                }
            }

            TypeColumnName[propertyInfo.GetHashCode()] = name;
            return name;
        }
        /// <summary>
        /// Specify a custom Column name mapper based on the POCO type name
        /// </summary>
        public static ClickHouseColumnMapperDelegate ClickHouseColumnMapper;
        private static ClickHouseColumnAttribute GetClickHouseColumn(PropertyInfo propertyInfo)
        {
            ;
            if (TypeClickHouseColumn.TryGetValue(propertyInfo.GetHashCode(), out ClickHouseColumnAttribute columnAttribute)) return columnAttribute;
            if (ClickHouseColumnMapper != null)
            {
                columnAttribute = ClickHouseColumnMapper(propertyInfo);
            }
            else
            {
                var info = propertyInfo;
                //NOTE: This as dynamic trick falls back to handle both our own Column-attribute as well as the one in EntityFramework 
                var columnAtt =
                    info.GetCustomAttribute<ClickHouseColumnAttribute>(false);
                return columnAtt;
            }

            TypeClickHouseColumn[propertyInfo.GetHashCode()] = columnAttribute;
            return columnAttribute;
        }

        private static (string, DynamicParameters) GenerateBulkSql<T>(IDbConnection connection, IEnumerable<T> entityToInsert, string tableName = null)
        {
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

            var name = !string.IsNullOrEmpty(tableName) ? tableName : GetTableName(type);
            var sbColumnList = new StringBuilder(null);
            var allProperties = TypePropertiesCache(type);
            var keyProperties = KeyPropertiesCache(type);
            var computedProperties = ComputedPropertiesCache(type);
            var allPropertiesExceptKeyAndComputed =
                allProperties.Except(keyProperties.Union(computedProperties)).ToList();

            for (var i = 0; i < allPropertiesExceptKeyAndComputed.Count; i++)
            {
                var property = allPropertiesExceptKeyAndComputed[i];
                var clickhouseColumn = GetClickHouseColumn(property);

                if (clickhouseColumn?.IsOnlyIgnoreInsert == true)
                    continue;
                sbColumnList.AppendFormat("{0}", GetColumnName(property));
                if (i < allPropertiesExceptKeyAndComputed.Count - 1)
                    sbColumnList.Append(", ");
            }

            var sbParameterList = new StringBuilder(null);
            DynamicParameters parameters = new DynamicParameters();

            for (int j = 0, length = Enumerable.Count(entityToInsert); j < length; j++)
            {
                var item = Enumerable.ElementAt(entityToInsert,j);
                {
                    sbParameterList.Append("(");
                    for (var i = 0; i < allPropertiesExceptKeyAndComputed.Count; i++)
                    {
                        var property = allPropertiesExceptKeyAndComputed[i];

                        var clickhouseColumn = GetClickHouseColumn(property);

                        if (clickhouseColumn?.IsOnlyIgnoreInsert == true)
                            continue;

                        var val = property.GetValue(item);
                        var columnName = $"@{GetColumnName(property)}{j}{i}";
                        sbParameterList.Append(columnName);
                        if (property.PropertyType.IsValueType)
                        {
                            if (property.PropertyType == typeof(DateTime))
                            {
                                var datetimevalue = Convert.ToDateTime(val);
                                if (property.GetCustomAttribute<DateAttribute>() != null)
                                {
                                    parameters.Add(columnName, datetimevalue.Date, DbType.Date);
                                    //sbParameterList.AppendFormat("'{0:yyyy-MM-dd}'", val);
                                }
                                else
                                {
                                    parameters.Add(columnName, datetimevalue, DbType.DateTime);
                                    //sbParameterList.AppendFormat("'{0:yyyy-MM-dd HH:mm:ss}'", val);
                                }
                            }
                            else if (property.PropertyType == typeof(Boolean))
                            {
                                var boolvalue = Convert.ToBoolean(val);
                                
                                {
                                    parameters.Add(columnName, boolvalue?1:0, DbType.Int32);
                                    //sbParameterList.AppendFormat("'{0:yyyy-MM-dd HH:mm:ss}'", val);
                                }
                            }
                            else
                            {
                                parameters.Add(columnName, val);
                                //sbParameterList.AppendFormat("{0}", FixValue(val));
                            }
                        }
                        else
                        {
                            parameters.Add(columnName, val);
                            ///sbParameterList.AppendFormat("'{0}'", FixValue(val));
                        }

                        if (i < allPropertiesExceptKeyAndComputed.Count - 1)
                            sbParameterList.Append(", ");
                    }

                    sbParameterList.Append("),");
                }
            }
            

            sbParameterList.Remove(sbParameterList.Length - 1, 1);
            sbParameterList.Append(";");
            //insert list of entities
            var cmd = $"insert into {name} ({sbColumnList}) values {sbParameterList}";
            return (cmd,parameters);
        }

        
        /// <summary>
        /// Inserts an entity into table "Ts" and returns identity id or number of inserted rows if inserting a list.
        /// </summary>
        /// <param name="connection">Open SqlConnection</param>
        /// <param name="entityToInsert">Entity to insert, can be list of entities</param>
        /// <returns>Identity of inserted entity, or number of inserted rows if inserting a list</returns>
        public static void InsertBulk<T>(this ClickHouse.Client.ADO.ClickHouseConnection connection, IEnumerable<T> entityToInsert, int? commandTimeout = null, string tableName = null)
        {
            InsertBulkAsync(connection, entityToInsert, commandTimeout, tableName).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Inserts an entity into table "Ts" and returns identity id or number of inserted rows if inserting a list.
        /// </summary>
        /// <param name="connection">Open SqlConnection</param>
        /// <param name="entityToInsert">Entity to insert, can be list of entities</param>
        /// <returns>Identity of inserted entity, or number of inserted rows if inserting a list</returns>
        public static void InsertBulk<T>(this IDbConnection connection, IEnumerable<T> entityToInsert, int? commandTimeout = null, string tableName = null)
        {
            var cmd = GenerateBulkSql(connection, entityToInsert, tableName);

            var wasClosed = connection.State == ConnectionState.Closed;
            if (wasClosed) connection.Open();

            connection.Execute(cmd.Item1, cmd.Item2, null, commandTimeout);
            if (wasClosed) connection.Close();
        }
        
        private static (string, List<dynamic[]>) GenerateCHBulkSql<T>(IDbConnection connection, IEnumerable<T> entityToInsert, string tableName = null)
        {
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

            var name = !string.IsNullOrEmpty(tableName) ? tableName : GetTableName(type);
            var sbColumnList = new StringBuilder(null);
            var allProperties = TypePropertiesCache(type);
            var keyProperties = KeyPropertiesCache(type);
            var computedProperties = ComputedPropertiesCache(type);
            var allPropertiesExceptKeyAndComputed =
                allProperties.Except(keyProperties.Union(computedProperties)).ToList();

            for (var i = 0; i < allPropertiesExceptKeyAndComputed.Count; i++)
            {
                var property = allPropertiesExceptKeyAndComputed[i];

                var  clickhouseColumn = GetClickHouseColumn(property);

                if (clickhouseColumn?.IsOnlyIgnoreInsert == true)
                    continue;

                sbColumnList.AppendFormat("{0}", GetColumnName(property));
                if (i < allPropertiesExceptKeyAndComputed.Count - 1)
                    sbColumnList.Append(", ");
            }

            //var sbParameterList = new StringBuilder(null);
            List<dynamic[]> dynamics = new List<dynamic[]>();

            for (int j = 0, length = Enumerable.Count(entityToInsert); j < length; j++)
            {
                var item = Enumerable.ElementAt(entityToInsert, j);
                {
                    List<dynamic> dynamicsParams = new List<dynamic>();
                    //sbParameterList.Append("(");
                    for (int i = 0,count= allPropertiesExceptKeyAndComputed.Count; i < count; i++)
                    {
                       
                        var property = allPropertiesExceptKeyAndComputed[i];

                        var clickhouseColumn = GetClickHouseColumn(property);

                        if (clickhouseColumn?.IsOnlyIgnoreInsert == true)
                            continue;

                        var val = property.GetValue(item);
                        //var columnName = $"@{GetColumnName(property)}{j}{i}";
                        //sbParameterList.Append(columnName);
                        if (property.PropertyType.IsValueType)
                        {
                            if (property.PropertyType == typeof(DateTime))
                            {
                                var datetimevalue = Convert.ToDateTime(val);
                                if (property.GetCustomAttribute<DateAttribute>() != null)
                                {
                                    //parameters.Add(columnName, datetimevalue.Date, DbType.Date);
                                    dynamicsParams.Add(datetimevalue.Date);
                                    //sbParameterList.AppendFormat("'{0:yyyy-MM-dd}'", val);
                                }
                                else
                                {
                                    dynamicsParams.Add(datetimevalue);
                                    //parameters.Add(columnName, datetimevalue, DbType.DateTime);
                                    //sbParameterList.AppendFormat("'{0:yyyy-MM-dd HH:mm:ss}'", val);
                                }
                            }
                            else if (property.PropertyType == typeof(Boolean))
                            {
                                var boolvalue = Convert.ToBoolean(val);

                                {
                                    //parameters.Add(columnName, boolvalue ? 1 : 0, DbType.Int32);
                                    dynamicsParams.Add(boolvalue);
                                    //sbParameterList.AppendFormat("'{0:yyyy-MM-dd HH:mm:ss}'", val);
                                }
                            }
                            else
                            {
                                dynamicsParams.Add(val);
                                //sbParameterList.AppendFormat("{0}", FixValue(val));
                            }
                        }
                        else
                        {
                            dynamicsParams.Add(val);
                            ///sbParameterList.AppendFormat("'{0}'", FixValue(val));
                        }

                        //if (i < allPropertiesExceptKeyAndComputed.Count - 1)
                        //    sbParameterList.Append(", ");
                       
                    }
                    dynamics.Add(dynamicsParams.ToArray());
                    //sbParameterList.Append("),");
                }
            }


            //sbParameterList.Remove(sbParameterList.Length - 1, 1);
            //sbParameterList.Append(";");
            //insert list of entities
            var cmd = $"insert into {name} ({sbColumnList}) values  @bulk; ";
            return (cmd, dynamics);
        }
        /// <summary>
        /// Inserts an entity into table "Ts" and returns identity id or number of inserted rows if inserting a list.
        /// </summary>
        /// <param name="connection">Open SqlConnection</param>
        /// <param name="entityToInsert">Entity to insert, can be list of entities</param>
        /// <returns>Identity of inserted entity, or number of inserted rows if inserting a list</returns>
        public static void InsertBulk<T>(this ClickHouse.Ado.ClickHouseConnection connection, IEnumerable<T> entityToInsert, int? commandTimeout = null, string tableName = null)
        {
            var cmd = GenerateCHBulkSql(connection, entityToInsert, tableName);

            var wasClosed = connection.State == ConnectionState.Closed;
            if (wasClosed) connection.Open();

            var command = connection.CreateCommand();
            command.CommandText = cmd.Item1;
            command.Parameters.Add(new ClickHouse.Ado.ClickHouseParameter
            {
                ParameterName = "bulk",
                Value = cmd.Item2
            });
            command.ExecuteNonQuery();
            if (wasClosed) connection.Close();

        }

        /// <summary>
        /// Specifies a custom callback that detects the database type instead of relying on the default strategy (the name of the connection type object).
        /// Please note that this callback is global and will be used by all the calls that require a database specific adapter.
        /// </summary>
        public static GetDatabaseTypeDelegate GetDatabaseType;

    }

    /// <summary>
    /// Defines the name of a table to use in Dapper.Contrib commands.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class)]
    public class TableAttribute : Attribute
    {
        /// <summary>
        /// Creates a table mapping to a specific name for Dapper.Contrib commands
        /// </summary>
        /// <param name="tableName">The name of this table in the database.</param>
        public TableAttribute(string tableName)
        {
            Name = tableName;
        }

        /// <summary>
        /// The name of the table in the database
        /// </summary>
        public string Name { get; set; }
    }

    /// <summary>
    /// Specifies that this field is a primary key in the database
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class KeyAttribute : Attribute
    {
    }

    /// <summary>
    /// Specifies that this field is a explicitly set primary key in the database
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class ExplicitKeyAttribute : Attribute
    {
    }

    /// <summary>
    /// Specifies whether a field is writable in the database.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class WriteAttribute : Attribute
    {
        /// <summary>
        /// Specifies whether a field is writable in the database.
        /// </summary>
        /// <param name="write">Whether a field is writable in the database.</param>
        public WriteAttribute(bool write)
        {
            Write = write;
        }

        /// <summary>
        /// Whether a field is writable in the database.
        /// </summary>
        public bool Write { get; }
    }

    /// <summary>
    /// Specifies that this is a computed column.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class ComputedAttribute : Attribute
    {
    }

    /// <summary>
    /// Specifies that this is a computed column.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class DateAttribute : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class ColumnNameAttribute : Attribute
    {
        /// <summary>
        /// Creates a ColumnName mapping to a specific name for Dapper.Contrib commands
        /// </summary>
        /// <param name="ColumnName">The name of this Column in the database.</param>
        public ColumnNameAttribute(string ColumnName)
        {
            Name = ColumnName;
        }

        /// <summary>
        /// The name of the Column Name in the database
        /// </summary>
        public string Name { get; set; }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class ClickHouseColumnAttribute : Attribute
    {
        /// <summary>
        /// The name of the Column Name in the database
        /// </summary>
        public string Name { get; set; }
        public bool IsOnlyIgnoreInsert { get; set; }
    }
}