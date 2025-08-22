using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using QueueServer.Core.Broker;
using QueueServer.Core.Configuration;
using QueueServer.Core.Network;
using QueueServer.Core.Protocol;
using QueueServer.Core.Storage;
using QueueServer.Core.Subscriptions;

namespace QueueServer.Core.Extensions;

/// <summary>
/// Dependency injection extensions for queue server components
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Add queue server services to the dependency injection container
    /// </summary>
    public static IServiceCollection AddQueueServer(this IServiceCollection services)
    {
        // Core services
        services.AddSingleton<SequentialStorageManager>();
        services.AddSingleton<SubscriptionManager>();
        services.AddSingleton<ProtocolHandler>();
        services.AddSingleton<TcpServer>();
        services.AddSingleton<MessageBroker>();

        return services;
    }

    /// <summary>
    /// Add configuration options from the configuration system
    /// </summary>
    public static IServiceCollection AddQueueServerConfiguration(
        this IServiceCollection services,
        Microsoft.Extensions.Configuration.IConfiguration configuration)
    {
        services.Configure<ServerOptions>(configuration.GetSection(ServerOptions.SectionName));
        services.Configure<StorageOptions>(configuration.GetSection(StorageOptions.SectionName));
        services.Configure<TopicOptions>(configuration.GetSection(TopicOptions.SectionName));
        services.Configure<ClientOptions>(configuration.GetSection(ClientOptions.SectionName));

        return services;
    }
}
