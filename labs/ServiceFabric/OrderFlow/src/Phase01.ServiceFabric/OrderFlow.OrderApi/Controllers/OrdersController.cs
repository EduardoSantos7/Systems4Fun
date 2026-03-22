namespace OrderFlow.OrderApi.Controllers;

using Microsoft.AspNetCore.Mvc;
using OrderFlow.Contracts.Api;
using OrderFlow.OrderApi.Services;

[ApiController]
[Route("api/[controller]")]
public class OrdersController(OrderService orderService) : ControllerBase
{
    private readonly OrderService _orderService = orderService;

    [HttpPost]
    public async Task<IActionResult> PlaceOrder([FromBody] PlaceOrderRequest request)
    {
        var order = await _orderService.PlaceOrderAsync(
            request.CustomerId, request.ProductId, request.Quantity);
        return Ok(order);
    }

    [HttpGet("{orderId:guid}")]
    public IActionResult GetOrder(Guid orderId)
    {
        var order = _orderService.GetOrder(orderId);
        return order is null ? NotFound() : Ok(order);
    }
}