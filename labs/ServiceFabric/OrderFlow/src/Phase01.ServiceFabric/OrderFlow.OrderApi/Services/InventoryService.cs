// Namespace change: OrderFlow.OrderApi.Services instead of OrderFlow.Monolith.Services
// Everything else is identical to Phase 0. The state is still in-memory.
// We are not solving durability in this phase.
namespace OrderFlow.OrderApi.Services;

using OrderFlow.Domain.Inventory;

public class InventoryService
{
    private readonly Dictionary<string, InventoryItem> _inventory = new()
    {
        ["PROD-001"] = new InventoryItem
        {
            ProductId = "PROD-001", ProductName = "Mechanical Keyboard",
            StockCount = 100, UnitPrice = 149.99m
        },
        ["PROD-002"] = new InventoryItem
        {
            ProductId = "PROD-002", ProductName = "USB-C Hub",
            StockCount = 250, UnitPrice = 49.99m
        },
    };

    private readonly object _lock = new();

    public bool TryReserveStock(string productId, int quantity)
    {
        lock (_lock)
        {
            if (!_inventory.TryGetValue(productId, out var item)) return false;
            if (item.StockCount < quantity) return false;
            item.StockCount -= quantity;
            return true;
        }
    }

    public InventoryItem? GetItem(string productId)
    {
        lock (_lock)
        {
            _inventory.TryGetValue(productId, out var item);
            return item;
        }
    }

    public IEnumerable<InventoryItem> GetAll()
    {
        lock (_lock) { return _inventory.Values.ToList(); }
    }
}