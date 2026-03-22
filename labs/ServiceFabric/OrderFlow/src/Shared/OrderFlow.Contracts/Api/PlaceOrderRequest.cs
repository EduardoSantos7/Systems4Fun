namespace OrderFlow.Contracts.Api;

public record PlaceOrderRequest(string CustomerId, string ProductId, int Quantity);