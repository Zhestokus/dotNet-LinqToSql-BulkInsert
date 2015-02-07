using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using LinqToSqlBulkInsertConsoleApp.DAL;

namespace LinqToSqlBulkInsertConsoleApp
{
    class Program
    {
        static void Main(string[] args)
        {
            var connectionString = "";

            using (var db = new StockDataContext(connectionString))
            {
                var products = ImportProducts();

                using (var bulkCopy = new ObjectBulkCopy<Product>((SqlConnection) db.Connection))
                {
                    bulkCopy.WriteToServer(products);
                }
            }
        }

        static IEnumerable<Product> ImportProducts()
        {
            //e.g import products from Excel
            return Enumerable.Empty<Product>();
        }
    }
}
