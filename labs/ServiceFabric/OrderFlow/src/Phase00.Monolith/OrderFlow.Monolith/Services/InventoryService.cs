using OrderFlow.Domain.Inventory;

namespace OrderFlow.Monolith.Services;

// WHY: This is the naive, simplest possible implementation.
// It works fine for one process on one machine.
// Watch what breaks when we need more than one instance.
public class InventoryService
{
    // This dictionary IS the state. It lives in process memory.
    // If the process crashes, all inventory data is gone.
    // If you start a second copy of this app for load balancing,
    // each copy has its own separate dictionary — they will diverge.
    private readonly Dictionary<string, InventoryItem> _inventory = new()
    {
        ["PROD-001"] = new InventoryItem
        {
            ProductId = "PROD-001",
            ProductName = "Mechanical Keyboard",
            StockCount = 100,
            UnitPrice = 149.99m
        },
        ["PROD-002"] = new InventoryItem
        {
            ProductId = "PROD-002",
            ProductName = "USB-C Hub",
            StockCount = 250,
            UnitPrice = 49.99m
        },
    };

    // The lock is necessary even in a single process because
    // ASP.NET Core handles requests on multiple threads.
    private readonly object _lock = new();

    public bool TryReserveStock(string productId, int quantity)
    {
        lock (_lock)
        {
            if (!_inventory.TryGetValue(productId, out var item))
                return false;

            if (item.StockCount < quantity)
                return false;

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
        lock (_lock)
        {
            return _inventory.Values.ToList();
        }
    }
}