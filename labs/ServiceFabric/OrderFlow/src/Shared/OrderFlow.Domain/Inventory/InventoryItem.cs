namespace OrderFlow.Domain.Inventory;

public class InventoryItem
{
    public string ProductId { get; set; } = string.Empty;
    public string ProductName { get; set; } = string.Empty;
    public int StockCount { get; set; }
    public decimal UnitPrice { get; set; }
}