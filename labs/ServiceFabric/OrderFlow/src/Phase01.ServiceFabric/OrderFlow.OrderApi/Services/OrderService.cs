namespace OrderFlow.OrderApi.Services;

using OrderFlow.Domain.Orders;

public class OrderService
{
    private readonly Dictionary<Guid, Order> _orders = new();
    private readonly object _lock = new();
    private readonly InventoryService _inventoryService;
    private readonly PaymentService _paymentService;

    public OrderService(InventoryService inventoryService, PaymentService paymentService)
    {
        _inventoryService = inventoryService;
        _paymentService = paymentService;
    }

    public async Task<Order> PlaceOrderAsync(string customerId, string productId, int quantity)
    {
        var item = _inventoryService.GetItem(productId)
            ?? throw new ArgumentException($"Product {productId} not found");

        var order = new Order
        {
            CustomerId = customerId,
            ProductId = productId,
            Quantity = quantity,
            TotalPrice = item.UnitPrice * quantity
        };

        lock (_lock) { _orders[order.Id] = order; }

        if (!_inventoryService.TryReserveStock(productId, quantity))
        {
            order.Status = OrderStatus.Failed;
            return order;
        }
        order.Status = OrderStatus.InventoryReserved;

        if (!await _paymentService.ProcessPaymentAsync(order.Id, order.TotalPrice))
        {
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