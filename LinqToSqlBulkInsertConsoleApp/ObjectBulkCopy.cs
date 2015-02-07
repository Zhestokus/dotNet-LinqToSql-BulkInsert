using System;
using System.Collections.Generic;
using System.Data.Linq;
using System.Data.Linq.Mapping;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace LinqToSqlBulkInsertConsoleApp
{
    public class ObjectBulkCopy<TEntity> : IDisposable
    {
        public event EventHandler<SqlRowsCopiedEventArgs> SqlRowsCopied;
        protected virtual void OnSqlRowsCopied(SqlRowsCopiedEventArgs e)
        {
            if (SqlRowsCopied != null)
            {
                SqlRowsCopied(this, e);
            }
        }

        private readonly String _tableName;
        private readonly IDictionary<String, String> _propertyToColumns;

        private readonly String _connectionString;

        private readonly SqlConnection _connection;
        private readonly SqlTransaction _transaction;

        private readonly bool _externalConnection;

        public ObjectBulkCopy(String connectionString)
            : this(new SqlConnection(connectionString), null, false)
        {
        }
        public ObjectBulkCopy(SqlConnection connection)
            : this(connection, null, true)
        {
        }

        public ObjectBulkCopy(SqlConnection connection, SqlTransaction transaction)
            : this(connection, transaction, true)
        {
        }

        private ObjectBulkCopy(SqlConnection connection, SqlTransaction transaction, bool externalConnection)
        {
            ObjectType = typeof(TEntity);

            _connection = connection;
            _transaction = transaction;

            _connectionString = connection.ConnectionString;
            _externalConnection = externalConnection;

            _tableName = GetDbTableName(ObjectType);
            _propertyToColumns = new Dictionary<String, String>();

            var properties = ObjectType.GetProperties();
            foreach (var propertyInfo in properties)
            {
                var columnAttribute = Attribute.GetCustomAttribute(propertyInfo, typeof(ColumnAttribute)) as ColumnAttribute;
                if (columnAttribute == null)
                    continue;

                var columnName = (!String.IsNullOrEmpty(columnAttribute.Name) ? columnAttribute.Name : propertyInfo.Name);
                _propertyToColumns[propertyInfo.Name] = columnName;
            }
        }

        public int BatchSize { get; set; }

        public int NotifyAfter { get; set; }

        public int BulkCopyTimeout { get; set; }

        public bool EnableStreaming { get; set; }

        public Type ObjectType { get; private set; }

        public String ConnectionString { get { return _connectionString; } }

        public void WriteToServer(IEnumerable<TEntity> collection)
        {
            WriteToServer(collection, SqlBulkCopyOptions.Default);
        }
        public void WriteToServer(IEnumerable<TEntity> collection, SqlBulkCopyOptions copyOptions)
        {
            using (var sqlBulkCopy = CreateSqlBulkCopy(copyOptions))
            {
                foreach (var pair in _propertyToColumns)
                    sqlBulkCopy.ColumnMappings.Add(pair.Value, pair.Key);

                sqlBulkCopy.BatchSize = BatchSize;
                sqlBulkCopy.NotifyAfter = NotifyAfter;
                sqlBulkCopy.BulkCopyTimeout = BulkCopyTimeout;
                sqlBulkCopy.EnableStreaming = EnableStreaming;
                sqlBulkCopy.DestinationTableName = _tableName;

                sqlBulkCopy.SqlRowsCopied += sqlBulkCopy_SqlRowsCopied;

                var reader = ObjectDataReader.Create(collection);
                sqlBulkCopy.WriteToServer(reader);

                sqlBulkCopy.Close();
            }
        }

        private void sqlBulkCopy_SqlRowsCopied(object sender, SqlRowsCopiedEventArgs args)
        {
            OnSqlRowsCopied(args);
        }

        private SqlBulkCopy CreateSqlBulkCopy(SqlBulkCopyOptions copyOptions)
        {
            if (_connection == null)
                return new SqlBulkCopy(_connectionString, copyOptions);

            if (_transaction == null)
                return new SqlBulkCopy(_connection);

            return new SqlBulkCopy(_connection, copyOptions, _transaction);
        }

        private String GetDbTableName(Type entityType)
        {
            if (entityType == null)
            {
                return null;
            }

            var tableAttribute = Attribute.GetCustomAttribute(entityType, typeof(TableAttribute)) as TableAttribute;
            if (tableAttribute == null)
            {
                return null;
            }

            return tableAttribute.Name;
        }

        public void Dispose()
        {
            if (_externalConnection)
            {
                return;
            }

            if (_transaction != null)
            {
                _transaction.Commit();
                _transaction.Dispose();
            }

            if (_connection != null)
            {
                _connection.Dispose();
            }
        }
    }
}
