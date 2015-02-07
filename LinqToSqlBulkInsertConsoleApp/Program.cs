using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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
                
                var bulkCopy = new ObjectBulkCopy<Product>();
                bulkCopy.WriteToServer(products);

            }
            


        }

        static IEnumerable<Product> ImportProducts()
        {
            //e.g import products from Excel
            return Enumerable.Empty<Product>();
        }
    }


    
}
