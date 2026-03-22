using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.ServiceFabric.Services.Communication.AspNetCore;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using OrderFlow.OrderApi.Services; 
using System.Fabric;

// OrderApi IS your service from Service Fabric's perspective.
// SF calls methods on this class to manage the service lifecycle.
// You do not instantiate it — SF does, using the factory in Program.cs.
internal sealed class OrderApi(StatelessServiceContext context) : StatelessService(context)
{

    // CreateServiceInstanceListeners is the lifecycle hook SF calls
    // when it is ready for your service to start accepting connections.
    // You return a list of listeners — one per protocol/port you want to open.
    //
    // This is called:
    //   - On initial start
    //   - After a crash and restart
    //   - After a move to a new node (failover)
    //
    // SF guarantees it is called at the right moment in the lifecycle.
    // You must NOT open ports in the constructor or in Program.cs.
    protected override IEnumerable<ServiceInstanceListener> CreateServiceInstanceListeners()
    {
        return
        [
            new ServiceInstanceListener(serviceContext =>
                // KestrelCommunicationListener bridges SF's communication
                // lifecycle with ASP.NET Core's Kestrel server.
                //
                // "ServiceEndpoint" — must match the Endpoint Name in
                // PackageRoot/ServiceManifest.xml exactly.
                //
                // SF uses this name to look up the port assignment from
                // the manifest and register the URL with the Naming Service.
                new KestrelCommunicationListener(
                    serviceContext,
                    "ServiceEndpoint",
                    (url, listener) =>
                    {
                        // url is the address SF computed for this listener.
                        // It is based on the node IP + the port from the manifest.
                        // SF registers this url in the Naming Service so other
                        // services can find this instance.
                        //
                        // Example: "http://10.0.0.1:8080"
                        ServiceEventSource.Current.ServiceMessage(
                            serviceContext,
                            $"Starting Kestrel on {url}");

                        var builder = WebApplication.CreateBuilder();

                        // Make the SF context available to controllers/services
                        // via DI. Useful for logging the node name, instance ID, etc.
                        builder.Services.AddSingleton(serviceContext);

                        // --- Business logic services (same as Phase 0 monolith) ---
                        // These are still in-memory. We are not solving the state
                        // durability problem yet — that is Phase 2.
                        // Right now we are proving that the SF hosting layer works.
                        builder.Services.AddSingleton<InventoryService>();
                        builder.Services.AddSingleton<PaymentService>();
                        builder.Services.AddSingleton<OrderService>();
                        // ----------------------------------------------------------

                        builder.Services.AddControllers();

                        builder.WebHost
                            .UseKestrel()
                            .UseContentRoot(Directory.GetCurrentDirectory())
                            // UseServiceFabricIntegration adds a middleware that
                            // validates incoming requests are for THIS specific
                            // partition/instance. Prevents stale routing from
                            // calling the wrong replica. Especially important for
                            // stateful services — we add it now as a habit.
                            .UseServiceFabricIntegration(
                                listener,
                                ServiceFabricIntegrationOptions.None)
                            .UseUrls(url);

                        var app = builder.Build();
                        app.MapControllers();
                        return app;
                    }), "HttpEndpoint")
        ];
    }
}