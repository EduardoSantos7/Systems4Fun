using System.Diagnostics.Tracing;
using System.Fabric;
using System.Threading.Tasks;

// ServiceEventSource is an ETW (Event Tracing for Windows) event source.
// Service Fabric uses ETW as its primary diagnostic channel.
//
// WHY THIS EXISTS: SF's health and diagnostic pipeline knows how to
// collect ETW events from all services across all nodes and funnel
// them into Azure Monitor / Application Insights / local diagnostics.
// Without this, your service is a black box from SF's perspective.
//
// You must have at least the two events used in Program.cs:
//   ServiceTypeRegistered and ServiceHostInitializationFailed
//
// In Phase 6 (Observability) we expand this significantly.
// For now, treat it as required boilerplate.
[EventSource(Name = "OrderFlow-Phase01-OrderApi")]
internal sealed class ServiceEventSource : EventSource
{
    // Singleton pattern: ETW event sources must be singletons.
    public static readonly ServiceEventSource Current = new();

    static ServiceEventSource()
    {
        // Ensure Tasks type is loaded before tracing starts.
        // Required by ETW infrastructure.
        Task.Run(() => { });
    }

    private ServiceEventSource() : base() { }

    // -----------------------------------------------------------------------
    // Events used by Program.cs
    // -----------------------------------------------------------------------

    private const int ServiceTypeRegisteredEventId = 3;
    [Event(ServiceTypeRegisteredEventId, Level = EventLevel.Informational,
        Message = "Service host process {0} registered service type {1}")]
    public void ServiceTypeRegistered(int hostProcessId, string serviceType)
    {
        WriteEvent(ServiceTypeRegisteredEventId, hostProcessId, serviceType);
    }

    private const int ServiceHostInitializationFailedEventId = 4;
    [Event(ServiceHostInitializationFailedEventId, Level = EventLevel.Error,
        Message = "Service host initialization failed with exception: {0}")]
    public void ServiceHostInitializationFailed(string exception)
    {
        WriteEvent(ServiceHostInitializationFailedEventId, exception);
    }

    // -----------------------------------------------------------------------
    // General-purpose service message (used in OrderApi.cs)
    // -----------------------------------------------------------------------

    private const int ServiceMessageEventId = 2;
    [Event(ServiceMessageEventId, Level = EventLevel.Informational,
        Message = "{7}")]
    private void ServiceMessage(
        string serviceName, string serviceTypeName, long replicaOrInstanceId,
        Guid partitionId, string applicationName, string applicationTypeName,
        string nodeName, string message)
    {
        WriteEvent(ServiceMessageEventId, serviceName, serviceTypeName,
            replicaOrInstanceId, partitionId, applicationName,
            applicationTypeName, nodeName, message);
    }

    // Convenience overload that extracts context fields automatically.
    [NonEvent]
    public void ServiceMessage(StatelessServiceContext context, string message)
    {
        ServiceMessage(
            context.ServiceName.ToString(),
            context.ServiceTypeName,
            context.InstanceId,
            context.PartitionId,
            context.CodePackageActivationContext.ApplicationName,
            context.CodePackageActivationContext.ApplicationTypeName,
            context.NodeContext.NodeName,
            message);
    }
}