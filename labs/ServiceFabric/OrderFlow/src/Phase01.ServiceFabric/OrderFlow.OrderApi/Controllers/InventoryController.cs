namespace OrderFlow.OrderApi.Controllers;

using Microsoft.AspNetCore.Mvc;
using OrderFlow.OrderApi.Services;

[ApiController]
[Route("api/[controller]")]
public class InventoryController(InventoryService inventoryService) : ControllerBase
{
    private readonly InventoryService _inventoryService = inventoryService;

    [HttpGet]
    public IActionResult GetAll() => Ok(_inventoryService.GetAll());

    [HttpGet("{productId}")]
    public IActionResult GetItem(string productId)
    {
        var item = _inventoryService.GetItem(productId);
        return item is null ? NotFound() : Ok(item);
    }
}