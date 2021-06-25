using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace ClickHouse.Ado.Client
{
    public class ClientContext
    {
        public AopEvents Aop;
        public ClickHouseConnection Connection;

        /// <summary>
        /// Execute parameterized SQL.
        /// </summary>
        /// <param name="cnn">The Connection to query on.</param>
        /// <param name="sql">The SQL to execute for this query.</param>
        /// <param name="param">The parameters to use for this query.</param>
        /// <param name="transaction">The transaction to use for this query.</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout.</param>
        /// <param name="commandType">Is it a stored proc or a batch?</param>
        /// <returns>The number of rows affected.</returns>
        public int Execute( string sql, ClickHouseParameter[] param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            var command = CreateCommand(this.Connection, sql, param, transaction, commandTimeout, commandType);
            return this.Execute(command);
        }

        /// <summary>
        /// Execute parameterized SQL.
        /// </summary>
        
        /// <param name="command">The command to execute on this Connection.</param>
        /// <returns>The number of rows affected.</returns>
        public int Execute(ClickHouseCommand command)
        {
            return OnAopExec<int>(command, (c) =>
            {
                return c.ExecuteNonQuery();
            });


        }
        /// <summary>
        /// Execute parameterized SQL that selects a single value.
        /// </summary>
        /// <typeparam name="T">The type to return.</typeparam>
        
        /// <param name="sql">The SQL to execute.</param>
        /// <param name="param">The parameters to use for this command.</param>
        /// <param name="transaction">The transaction to use for this command.</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout.</param>
        /// <param name="commandType">Is it a stored proc or a batch?</param>
        /// <returns>The first cell returned, as <typeparamref name="T"/>.</returns>
        public T ExecuteScalar<T>( string sql, ClickHouseParameter[] param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            var command = CreateCommand(Connection, sql, param, transaction, commandTimeout, commandType);
            return this.ExecuteScalar<T>(command);
        }
        /// <summary>
        /// Execute parameterized SQL that selects a single value.
        /// </summary>
        /// <typeparam name="T">The type to return.</typeparam>
        
        /// <param name="command">The command to execute.</param>
        /// <returns>The first cell selected as <typeparamref name="T"/>.</returns>
        public T ExecuteScalar<T>( ClickHouseCommand command)
        {

            command.Connection = Connection;


            return OnAopExec<T>(command, (c) =>
            {
                return (T)c.ExecuteScalar();
            });
           
        }
        /// <summary>
        /// Executes a query, returning the data typed as <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">The type of results to return.</typeparam>
        /// <param name="cnn">The Connection to query on.</param>
        /// <param name="sql">The SQL to execute for the query.</param>
        /// <param name="param">The parameters to pass, if any.</param>
        /// <param name="transaction">The transaction to use, if any.</param>
        /// <param name="buffered">Whether to buffer results in memory.</param>
        /// <param name="commandTimeout">The command timeout (in seconds).</param>
        /// <param name="commandType">The type of command to execute.</param>
        /// <returns>
        /// A sequence of data of the supplied type; if a basic type (int, string, etc) is queried then the data from the first column is assumed, otherwise an instance is
        /// created per row, and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        public IEnumerable<T> Query<T>( string sql, ClickHouseParameter[] param = null, IDbTransaction transaction = null, bool buffered = true, int? commandTimeout = null, CommandType? commandType = null)
        {
            var command = CreateCommand(Connection, sql, param, transaction, commandTimeout, commandType);
            return this.Query<T>(command);
        }
        /// <summary>
        /// Executes a query, returning the data typed as <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">The type of results to return.</typeparam>
        /// <param name="cnn">The Connection to query on.</param>
        /// <param name="command">The command used to query on this Connection.</param>
        /// <returns>
        /// A sequence of data of <typeparamref name="T"/>; if a basic type (int, string, etc) is queried then the data from the first column in assumed, otherwise an instance is
        /// created per row, and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        public IEnumerable<T> Query<T>( ClickHouseCommand command)
        {
            command.Connection = Connection;
            return OnAopExec<IEnumerable<T>>(command, (c) =>
            {
                IDataReader dr = c.ExecuteReader();
                using (dr)
                {
                    List<string> field = new List<string>(dr.FieldCount);
                    for (int i = 0; i < dr.FieldCount; i++)
                    {
                        field.Add(dr.GetName(i).ToLower());
                    }
                    List<T> list = new List<T>();
                    while (dr.NextResult())
                    {
                        while (dr.Read())
                        {
                            T model = Activator.CreateInstance<T>();
                            foreach (PropertyInfo property in model.GetType().GetProperties(BindingFlags.GetProperty | BindingFlags.Public | BindingFlags.Instance))
                            {
                                if (field.Contains(property.Name.ToLower()))
                                {
                                    if (!IsNullOrDBNull(dr[property.Name]))
                                    {
                                        property.SetValue(model, HackType(dr[property.Name], property.PropertyType), null);
                                    }
                                }
                            }
                            list.Add(model);
                        }
                    }
                    return list;
                }
            });
        }
        private ClickHouseCommand CreateCommand(ClickHouseConnection Connection, string sql, ClickHouseParameter[] param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            var command = Connection.CreateCommand();
            command.CommandText = sql;
            command.Transaction = transaction;
            if (commandTimeout.HasValue)
                command.CommandTimeout = commandTimeout.Value;
            if (commandType.HasValue)
                command.CommandType = commandType.Value;
            foreach (var item in param)
            {

                command.Parameters.Add(item);
            }
            return command;
        }
        //这个类对可空类型进行判断转换，要不然会报错
        private object HackType(object value, Type conversionType)
        {
            if (conversionType.IsGenericType && conversionType.GetGenericTypeDefinition().Equals(typeof(Nullable<>)))
            {
                if (value == null)
                    return null;

                System.ComponentModel.NullableConverter nullableConverter = new System.ComponentModel.NullableConverter(conversionType);
                conversionType = nullableConverter.UnderlyingType;
            }
            return Convert.ChangeType(value, conversionType);
        }

        private bool IsNullOrDBNull(object obj)
        {
            return ((obj is null) || (obj is DBNull) || string.IsNullOrEmpty(obj.ToString())) ? true : false;
        }
        private T OnAopExec<T>(ClickHouseCommand command, Func<ClickHouseCommand, T> func)
        {
            T result;
            try
            {
                this.Aop?.OnLogExecuting?.Invoke(command);
                result = func(command);
                this.Aop?.OnLogExecuted?.Invoke(command);
            }
            catch (Exception ex)
            {
                var clientEx = new ClientException(ex, command);
                this.Aop?.OnError?.Invoke(clientEx);
                throw ex;
            }
            return result;
        }
    }
}
