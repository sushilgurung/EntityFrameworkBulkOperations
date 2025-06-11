﻿using Gurung.BulkOperations.SqlDataHandler;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gurung.BulkOperations
{
    public static class BulkTransactionManager
    {
        public static async Task BulkInsertAsync<T>(
           this DbContext context,
            IEnumerable<T> entities,
            BulkConfig bulkConfig = null,
            CancellationToken cancellationToken = default
            )
        {
            //  bulkConfig.dataHandler = SqlDataHandlerFactory.CreateDataHandler(context);
            ISqlDataHandler sqlDataHandler = SqlDataHandlerFactory.CreateDataHandler(context);
            await sqlDataHandler.BulkInsertAsync(context, entities, bulkConfig, cancellationToken);
        }


        public static async Task BulkUpDateAsync<T>(
          this DbContext context,
          IEnumerable<T> entities,
          BulkConfig bulkConfig = null,
          CancellationToken cancellationToken = default
          )
        {
            ISqlDataHandler sqlDataHandler = bulkConfig.dataHandler;
            await sqlDataHandler.BulkUpDateAsync(context, entities, bulkConfig, cancellationToken);
        }

        public static async Task BulkInsertOrUpDateAsync<T>(
          this DbContext context,
           IEnumerable<T> entities,
           BulkConfig bulkConfig = null,
           CancellationToken cancellationToken = default)
        {
            ISqlDataHandler sqlDataHandler = bulkConfig.dataHandler;
            await sqlDataHandler.BulkInsertOrUpDateAsync(context, entities, bulkConfig, cancellationToken);
        }

    }
}
