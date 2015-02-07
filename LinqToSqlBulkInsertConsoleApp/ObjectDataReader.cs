using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Reflection;

namespace LinqToSqlBulkInsertConsoleApp
{
    public class ObjectDataReader : IDataReader
    {
        #region Static Methods

        public static ObjectDataReader Create<TObject>(IEnumerable<TObject> collection)
        {
            var type = typeof(TObject);
            return new ObjectDataReader(type, collection);
        }

        public static ObjectDataReader Create(Type type, IEnumerable collection)
        {
            return new ObjectDataReader(type, collection);
        }

        #endregion

        #region Private fields

        /// <summary>
        /// The enumerator for the IEnumerable{TData} passed to the constructor for 
        /// this instance.
        /// </summary>
        private readonly IEnumerator enumerator;

        /// <summary>
        /// The lookup of accessor functions for the properties on the TData type.
        /// </summary>
        private readonly PropertyInfo[] properties;

        /// <summary>
        /// The lookup of property names against their ordinal positions.
        /// </summary>
        private readonly IDictionary<String, int> ordinalLookup;

        #endregion

        #region Constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="ObjectDataReader"/> class.
        /// </summary>
        /// <param name="type">Type of objects in collection</param>
        /// <param name="collection">The data this instance should enumerate through.</param>
        private ObjectDataReader(Type type, IEnumerable collection)
        {
            enumerator = collection.GetEnumerator();

            // Get all the readable properties for the class and
            // compile an expression capable of reading it
            properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public);

            ordinalLookup = new Dictionary<String, int>();

            for (int i = 0; i < properties.Length; i++)
            {
                ordinalLookup.Add(properties[i].Name, i);
            }
        }

        #endregion

        #region IDataReader Members

        public void Close()
        {
            Dispose();
        }

        public int Depth
        {
            get { return 1; }
        }

        public DataTable GetSchemaTable()
        {
            return null;
        }

        public bool IsClosed
        {
            get { return enumerator == null; }
        }

        public bool NextResult()
        {
            return false;
        }

        public bool Read()
        {
            if (enumerator == null)
            {
                throw new ObjectDisposedException("ObjectDataReader");
            }

            return enumerator.MoveNext();
        }

        public int RecordsAffected
        {
            get { return -1; }
        }

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected void Dispose(bool disposing)
        {
            if (disposing)
            {
                var disposable = enumerator as IDisposable;
                if (disposable != null)
                {
                    disposable.Dispose();
                }
            }
        }

        #endregion

        #region IDataRecord Members

        public int FieldCount
        {
            get { return properties.Length; }
        }

        public bool GetBoolean(int i)
        {
            return (bool)GetValue(i);
        }

        public byte GetByte(int i)
        {
            return (byte)GetValue(i);
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferOffset, int length)
        {
            var array = (byte[])GetValue(i);

            var count = 0;
            while ((count++) < length)
            {
                if (fieldOffset >= array.Length)
                {
                    break;
                }

                buffer[bufferOffset++] = array[fieldOffset++];
            }

            return count;
        }

        public char GetChar(int i)
        {
            return (char)GetValue(i);
        }

        public long GetChars(int i, long fieldOffset, char[] buffer, int bufferOffset, int length)
        {
            var array = (char[])GetValue(i);

            var count = 0;
            while ((count++) < length)
            {
                if (fieldOffset >= array.Length)
                {
                    break;
                }

                buffer[bufferOffset++] = array[fieldOffset++];
            }

            return count;
        }

        public IDataReader GetData(int i)
        {
            var value = GetValue(i);
            if (value == null)
            {
                return null;
            }

            var type = (Type)null;
            var enumerable = value as IEnumerable;

            if (enumerable == null)
            {
                type = value.GetType();
                enumerable = new[] { value };
            }
            else
            {
                var enType = enumerable.GetType();
                if (enType.IsArray)
                {
                    type = enType.GetElementType();
                }
                else if (enType.IsGenericType)
                {
                    var genericArgs = enType.GetGenericArguments();
                    type = genericArgs[0];
                }
            }

            return new ObjectDataReader(type, enumerable);
        }

        public String GetDataTypeName(int i)
        {
            var value = GetValue(i);
            if (value == null)
            {
                return null;
            }

            return value.GetType().Name;
        }

        public DateTime GetDateTime(int i)
        {
            return (DateTime)GetValue(i);
        }

        public decimal GetDecimal(int i)
        {
            return (decimal)GetValue(i);
        }

        public double GetDouble(int i)
        {
            return (double)GetValue(i);
        }

        public Type GetFieldType(int i)
        {
            var value = GetValue(i);
            if (value == null)
            {
                return null;
            }

            return value.GetType();
        }

        public float GetFloat(int i)
        {
            return (float)GetValue(i);
        }

        public Guid GetGuid(int i)
        {
            return (Guid)GetValue(i);
        }

        public short GetInt16(int i)
        {
            return (short)GetValue(i);
        }

        public int GetInt32(int i)
        {
            return (int)GetValue(i);
        }

        public long GetInt64(int i)
        {
            return (long)GetValue(i);
        }

        public String GetName(int i)
        {
            return properties[i].Name;
        }

        public int GetOrdinal(String name)
        {
            int ordinal;
            if (!ordinalLookup.TryGetValue(name, out ordinal))
            {
                throw new InvalidOperationException("Unknown parameter name " + name);
            }

            return ordinal;
        }

        public String GetString(int i)
        {
            return (String)GetValue(i);
        }

        public Object GetValue(int i)
        {
            if (enumerator == null)
            {
                throw new ObjectDisposedException("ObjectDataReader");
            }

            var property = properties[i];
            return property.GetValue(enumerator.Current);
        }

        public int GetValues(Object[] values)
        {
            int i = 0;

            while (i < values.Length && i < FieldCount)
            {
                values[i] = GetValue(i);

                i++;
            }

            return i;
        }

        public bool IsDBNull(int i)
        {
            var value = GetValue(i);
            return (value == null || Convert.IsDBNull(value));
        }

        public Object this[String name]
        {
            get
            {
                var index = GetOrdinal(name);
                return GetValue(index);
            }
        }

        public Object this[int i]
        {
            get { return GetValue(i); }
        }

        #endregion
    }
}
