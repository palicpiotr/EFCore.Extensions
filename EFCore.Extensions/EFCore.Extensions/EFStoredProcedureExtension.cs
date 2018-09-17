using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace EFCore.Extensions
{
    public static class EFStoredProcedureExtension
    {

        public static DbCommand LoadStoredProcedure(this DbContext ctx, string name, bool prependDefaultSchema, short commandTimeout = 30)
        {
            var dbCommand = ctx.Database.GetDbConnection().CreateCommand();
            dbCommand.CommandTimeout = commandTimeout;
            if (prependDefaultSchema)
            {
                string schema = ctx.Model.Relational().DefaultSchema;
                name = !string.IsNullOrWhiteSpace(schema) ? $"{schema}.{name}" : name;
            }
            dbCommand.CommandText = name;
            dbCommand.CommandType = CommandType.StoredProcedure;
            return dbCommand;
        }

        public static DbCommand WithSqlParam(this DbCommand cmd, string paramName, object paramValue, Action<DbParameter> configureParam = null)
        {
            if (string.IsNullOrEmpty(cmd.CommandText) && cmd.CommandType != System.Data.CommandType.StoredProcedure)
                throw new InvalidOperationException("Call LoadStoredProc before using this method");
            var param = cmd.CreateParameter();
            param.ParameterName = paramName;
            param.Value = paramValue ?? DBNull.Value;
            configureParam?.Invoke(param);
            cmd.Parameters.Add(param);
            return cmd;
        }

        public static DbCommand WithSqlParam(this DbCommand cmd, string paramName, Action<DbParameter> configureParam = null)
        {
            if (string.IsNullOrEmpty(cmd.CommandText) && cmd.CommandType != CommandType.StoredProcedure)
                throw new InvalidOperationException("Call LoadStoredProc before using this method");
            var param = cmd.CreateParameter();
            param.ParameterName = paramName;
            configureParam?.Invoke(param);
            cmd.Parameters.Add(param);
            return cmd;
        }

        public static DbCommand WithSqlParam(this DbCommand cmd, string paramName, SqlParameter parameter)
        {
            if (string.IsNullOrEmpty(cmd.CommandText) && cmd.CommandType != CommandType.StoredProcedure)
                throw new InvalidOperationException("Call LoadStoredProc before using this method");
            cmd.Parameters.Add(parameter);
            return cmd;
        }

        public class DbResultMapper
        {

            private DbDataReader _reader;

            public DbResultMapper(DbDataReader reader) => _reader = reader;

            public IList<T> ReadToList<T>() => MapToList<T>(_reader);

            public T? ReadToValue<T>() where T : struct => MapToValue<T>(_reader);

            public Task<bool> NextResultAsync() => _reader.NextResultAsync();

            public Task<bool> NextResultAsync(CancellationToken ct) => _reader.NextResultAsync(ct);

            public bool NextResult() => _reader.NextResult();

            private IList<T> MapToList<T>(DbDataReader dr)
            {
                var objList = new List<T>();
                var props = typeof(T).GetRuntimeProperties().ToList();
                var colSchemaMapping = dr.GetColumnSchema()
                    .Where(x => props.Any(y => y.Name.ToLower() == x.ColumnName.ToLower()))
                    .ToDictionary(key => key.ColumnName.ToLower());
                if (dr.HasRows)
                {
                    while (dr.Read())
                    {
                        T obj = Activator.CreateInstance<T>();
                        foreach (var prop in props)
                        {
                            if (colSchemaMapping.ContainsKey(prop.Name.ToLower()))
                            {
                                var column = colSchemaMapping[prop.Name.ToLower()];
                                if (column?.ColumnOrdinal != null)
                                {
                                    var val = dr.GetValue(column.ColumnOrdinal.Value);
                                    prop.SetValue(obj, val == DBNull.Value ? null : val);
                                }
                            }
                        }
                        objList.Add(obj);
                    }
                }
                return objList;
            }

            private T? MapToValue<T>(DbDataReader dr) where T : struct
            {
                if (dr.HasRows)
                    if (dr.Read())
                        return dr.IsDBNull(0) ? new T?() : new T?(dr.GetFieldValue<T>(0));
                return new T?();
            }
        }

        public static void ExecuteStoredProc(this DbCommand command, Action<DbResultMapper> handleResults, System.Data.CommandBehavior commandBehaviour = System.Data.CommandBehavior.Default, bool manageConnection = true)
        {
            if (handleResults == null)
                throw new ArgumentNullException(nameof(handleResults));
            using (command)
            {
                if (manageConnection && command.Connection.State == System.Data.ConnectionState.Closed)
                    command.Connection.Open();
                try
                {
                    using (var reader = command.ExecuteReader(commandBehaviour))
                    {
                        handleResults(new DbResultMapper(reader));
                    }
                }
                finally
                {
                    if (manageConnection)
                        command.Connection.Close();
                }
            }
        }

        public async static Task ExecuteStoredProcAsync(this DbCommand command, Action<DbResultMapper> handleResults, System.Data.CommandBehavior commandBehaviour = System.Data.CommandBehavior.Default, CancellationToken ct = default(CancellationToken), bool manageConnection = true)
        {
            if (handleResults == null)
                throw new ArgumentNullException(nameof(handleResults));
            using (command)
            {
                if (manageConnection && command.Connection.State == ConnectionState.Closed)
                    await command.Connection.OpenAsync(ct).ConfigureAwait(false);
                try
                {
                    using (var reader = await command.ExecuteReaderAsync(commandBehaviour, ct).ConfigureAwait(false))
                    {
                        handleResults(new DbResultMapper(reader));
                    }
                }
                finally
                {
                    if (manageConnection)
                        command.Connection.Close();
                }
            }
        }

        public static int ExecuteStoredNonQuery(this DbCommand command, System.Data.CommandBehavior commandBehaviour = System.Data.CommandBehavior.Default, bool manageConnection = true)
        {
            int numberOfRecordsAffected = -1;

            using (command)
            {
                if (command.Connection.State == ConnectionState.Closed)
                    command.Connection.Open();
                try
                {
                    numberOfRecordsAffected = command.ExecuteNonQuery();
                }
                finally
                {
                    if (manageConnection)
                        command.Connection.Close();
                }
            }

            return numberOfRecordsAffected;
        }

        public async static Task<int> ExecuteStoredNonQueryAsync(this DbCommand command, CommandBehavior commandBehaviour = CommandBehavior.Default, CancellationToken ct = default(CancellationToken), bool manageConnection = true)
        {
            int numberOfRecordsAffected = -1;
            using (command)
            {
                if (command.Connection.State == System.Data.ConnectionState.Closed)
                    await command.Connection.OpenAsync(ct).ConfigureAwait(false);
                try
                {
                    numberOfRecordsAffected = await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                }
                finally
                {
                    if (manageConnection)
                        command.Connection.Close();
                }
            }
            return numberOfRecordsAffected;
        }

    }
}