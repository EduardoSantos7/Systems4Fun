namespace OrderFlow.Monolith.Services;

public class PaymentService
{
    // Simulates calling an external payment API.
    // In reality this would be Stripe, Adyen, etc.
    public async Task<bool> ProcessPaymentAsync(Guid orderId, decimal amount)
    {
        await Task.Delay(50); // Simulate network latency

        // Simulate occasional payment failure
        return Random.Shared.NextDouble() > 0.1; // 90% success rate
    }
}