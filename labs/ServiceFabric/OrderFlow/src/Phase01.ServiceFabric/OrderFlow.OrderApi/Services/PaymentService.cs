namespace OrderFlow.OrderApi.Services;

public class PaymentService
{
    public async Task<bool> ProcessPaymentAsync(Guid orderId, decimal amount)
    {
        await Task.Delay(50);
        return Random.Shared.NextDouble() > 0.1;
    }
}