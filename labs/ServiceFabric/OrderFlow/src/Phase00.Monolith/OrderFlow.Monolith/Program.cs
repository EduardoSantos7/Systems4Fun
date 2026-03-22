using OrderFlow.Monolith.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();

builder.Services.AddSingleton<InventoryService>();
builder.Services.AddSingleton<PaymentService>();
builder.Services.AddSingleton<OrderService>();

var app = builder.Build();

app.MapControllers();

app.Run();