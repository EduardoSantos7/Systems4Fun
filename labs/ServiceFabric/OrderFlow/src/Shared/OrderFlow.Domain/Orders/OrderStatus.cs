namespace OrderFlow.Domain.Orders;

public enum OrderStatus
{
    Pending,
    InventoryReserved,
    PaymentProcessed,
    Fulfilled,
    Failed
}