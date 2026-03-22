using Microsoft.AspNetCore.Mvc;
using OrderFlow.Monolith.Services;

namespace OrderFlow.Monolith.Controllers;

[ApiController]
[Route("api/[controller]")]
public class InventoryController(InventoryService inventoryService) : ControllerBase
{
    private readonly InventoryService _inventoryService = inventoryService;

    [HttpGet]
    public IActionResult GetAll()
    {
        return Ok(_inventoryService.GetAll());
    }

    [HttpGet("{productId}")]
    public IActionResult GetById(string productId)
    {
        var item = _inventoryService.GetItem(productId);
        return item == null ? NotFound() : Ok(item);
    }
}