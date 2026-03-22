// This is NOT a normal ASP.NET Core Program.cs.
// It does not build a WebApplication. It does not call app.Run().
//
// Its only job: tell the Service Fabric runtime "here is the factory
// function to create my service type."
//
// Service Fabric starts this executable, calls RegisterServiceAsync,
// and then manages the entire lifecycle from that point forward.
// The StatelessService class (OrderApi.cs) is where the real work happens.

using System.Diagnostics;
using Microsoft.ServiceFabric.Services.Runtime;

try
{
    // "OrderApiType" MUST exactly match the ServiceTypeName in
    // PackageRoot/ServiceManifest.xml. Case-sensitive.
    // If they don't match, the service will show as 'InBuild' in Explorer
    // indefinitely because the runtime cannot find the registered type.
    await ServiceRuntime.RegisterServiceAsync(
        "OrderApiType",
        context => new OrderApi(context));

    // The process must stay alive — SF will kill it when it wants to stop.
    // If this thread exits, the process exits, SF sees a crash, and
    // applies the restart backoff policy.
    await Task.Delay(Timeout.Infinite);
}
catch (Exception ex)
{
    // If registration fails (e.g., type name mismatch, DI error at startup),
    // log and rethrow so SF can see the non-zero exit code.
    ServiceEventSource.Current.ServiceHostInitializationFailed(ex.ToString());
    throw;
}