using OrderFlow.Domain.Orders;

namespace OrderFlow.Monolith.Services;

public class OrderService(InventoryService inventoryService, PaymentService paymentService)
{
    // Same problem: in-memory state, not durable, not shareable.
    private readonly Dictionary<Guid, Order> _orders = new();
    private readonly object _lock = new();
    private readonly InventoryService _inventoryService = inventoryService;
    private readonly PaymentService _paymentService = paymentService;

    public async Task<Order> PlaceOrderAsync(string customerId, string productId, int quantity)
    {
        var item = _inventoryService.GetItem(productId) ?? throw new ArgumentException($"Product {productId} not found");
        var order = new Order
        {
            CustomerId = customerId,
            ProductId = productId,
            Quantity = quantity,
            TotalPrice = item.UnitPrice * quantity,
            Status = OrderStatus.Pending
        };

        lock (_lock)
        {
            _orders[order.Id] = order;
        }

        // Step 1: Reserve inventory
        // PROBLEM: What if the process crashes right here?
        // Inventory is reserved, but the order is still Pending.
        // When we restart, we have no idea this happened.
        bool reserved = _inventoryService.TryReserveStock(productId, quantity);
        if (!reserved)
        {
            order.Status = OrderStatus.Failed;
            return order;
        }

        order.Status = OrderStatus.InventoryReserved;

        // Step 2: Process payment
        // PROBLEM: What if payment fails halfway?
        // We already decremented inventory. We need to compensate.
        // This is the distributed transaction problem in miniature.
        bool paid = await _paymentService.ProcessPaymentAsync(order.Id, order.TotalPrice);
        if (!paid)
        {
            // We should restore inventory here, but in a real crash,
            // this code might never run.
            order.Status = OrderStatus.Failed;
            return order;
        }

        order.Status = OrderStatus.Fulfilled;
        return order;
    }

    public Order? GetOrder(Guid orderId)
    {
        lock (_lock)
        {
            _orders.TryGetValue(orderId, out var order);
            return order;
        }
    }
}