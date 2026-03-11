using System;
using Minerva.DB_Server.Network.Protos;
using Minerva.DB_Server.QueryExecutor;
using Minerva.DB_Server.Transactions;

namespace Minerva.DB_Server.Benchmarks;

public class TxnPayment(ClientRequest query, ITransactionManager transactionManager) : ExecutionPlan(query, transactionManager)
{
    private Transaction _tx;

    public override Transaction Execute()
    {
        _tx = _transactionManager.CreateTransaction();

        _tx.Begin();
        var req = _query.Tpccp;

        int warehouseId = req.WID;
        int districtId = req.DID;
        int customerId = req.CID;
        string customerLastName = req.CLAST;
        decimal amount = req.AMOUNT;
        ProcessPayment(warehouseId, districtId, customerId, customerLastName, amount);

        _tx.Complete();


        return _tx;
    }

    public void ProcessPayment(int warehouseId, int districtId, int customerId, string customerLastName, decimal amount)
    {
        _tx.GetWarehouse(warehouseId, out var warehouse);
        _tx.GetDistrict(warehouseId, districtId, out var district);


        warehouse.W_YTD = warehouse.W_YTD + (double)amount;
        _tx.PutWarehouse(warehouse);

        district.D_YTD += (double)amount;
        _tx.PutDistrict(district);

        Customer customer;
        if (customerId != -1)
        {
            _tx.GetCustomer(warehouseId, districtId, customerId, out customer);
        }
        else
        {
            _tx.GetCustomerByLastName(warehouseId, districtId, customerLastName, out customer);
        }

        if (customer == null)
        {
            throw new Exception("Customer not found");
        }



        customer.C_BALANCE = customer.C_BALANCE - (double)amount;
        customer.C_YTD_PAYMENT = customer.C_YTD_PAYMENT + (double)amount;
        customer.C_PAYMENT_CNT = customer.C_PAYMENT_CNT + 1;
        _tx.PutCustomer(customer);

        var history = new History
        {
            H_C_ID = customer.C_ID,
            H_C_D_ID = districtId,
            H_C_W_ID = warehouseId,
            H_D_ID = districtId,
            H_W_ID = warehouseId,
            H_DATE = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
            H_AMOUNT = (double)amount,
            H_DATA = $"{warehouse.W_NAME}    {district.D_NAME}"
        };
        _tx.PutHistory(history);
    }
}

