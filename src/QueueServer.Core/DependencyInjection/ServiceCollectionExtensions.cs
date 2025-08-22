using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using QueueServer.Core.Broker;
using QueueServer.Core.Configuration;
using QueueServer.Core.Network;
using QueueServer.Core.Protocol;
using QueueServer.Core.Storage;
using QueueServer.Core.Subscriptions;

namespace QueueServer.Core.DependencyInjection;

/// <summary>
/// Extension methods for configuring Queue Server services
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Add Queue Server services to the dependency injection container
    /// </summary>
    public static IServiceCollection AddQueueServer(this IServiceCollection services, IConfiguration configuration)
    {
        // Configure options
        services.Configure<ServerOptions>(configuration.GetSection(ServerOptions.SectionName));
        services.Configure<StorageOptions>(configuration.GetSection(StorageOptions.SectionName));
        services.Configure<TopicOptions>(configuration.GetSection(TopicOptions.SectionName));
        services.Configure<ClientOptions>(configuration.GetSection(ClientOptions.SectionName));

        // Add core services
        services.AddSingleton<SequentialStorageManager>();
        services.AddSingleton<SubscriptionManager>();
        services.AddSingleton<ProtocolHandler>();
        services.AddSingleton<TcpServer>();
        services.AddSingleton<MessageBroker>();

        return services;
    }

    /// <summary>
    /// Add Queue Server with custom configuration
    /// </summary>
    public static IServiceCollection AddQueueServer(this IServiceCollection services,
        Action<ServerOptions>? configureServer = null,
        Action<StorageOptions>? configureStorage = null,
        Action<TopicOptions>? configureTopics = null,
        Action<ClientOptions>? configureClient = null)
    {
        // Configure options with defaults and custom overrides
        if (configureServer != null)
        {
            services.Configure(configureServer);
        }

        if (configureStorage != null)
        {
            services.Configure(configureStorage);
        }

        if (configureTopics != null)
        {
            services.Configure(configureTopics);
        }

        if (configureClient != null)
        {
            services.Configure(configureClient);
        }

        // Add core services
        services.AddSingleton<SequentialStorageManager>();
        services.AddSingleton<SubscriptionManager>();
        services.AddSingleton<ProtocolHandler>();
        services.AddSingleton<TcpServer>();
        services.AddSingleton<MessageBroker>();

        return services;
    }
}
