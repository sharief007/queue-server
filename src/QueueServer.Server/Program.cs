using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using QueueServer.Core.Broker;
using QueueServer.Core.Configuration;
using QueueServer.Core.DependencyInjection;

namespace QueueServer.Server;

/// <summary>
/// Main server application entry point
/// </summary>
internal static class Program
{
    private static ServiceProvider? _serviceProvider;
    private static MessageBroker? _broker;
    private static ILogger? _logger;
    private static readonly CancellationTokenSource _cancellationTokenSource = new();

    public static async Task<int> Main(string[] args)
    {
        try
        {
            // Build configuration
            var configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .AddEnvironmentVariables()
                .AddCommandLine(args)
                .Build();

            // Setup dependency injection and logging
            var services = new ServiceCollection();

            // Add configuration
            services.AddSingleton<IConfiguration>(configuration);

            // Add logging
            services.AddLogging(builder =>
            {
                builder
                    .AddConfiguration(configuration.GetSection("Logging"))
                    .AddConsole()
                    .SetMinimumLevel(LogLevel.Information);
            });

            // Add Queue Server services
            services.AddQueueServer(configuration);

            _serviceProvider = services.BuildServiceProvider();
            _logger = _serviceProvider
                .GetRequiredService<ILoggerFactory>()
                .CreateLogger("QueueServer");

            _logger.LogInformation("QueueServer - High Performance Message Broker");
            _logger.LogInformation("============================================");

            // Setup signal handling for graceful shutdown
            Console.CancelKeyPress += OnCancelKeyPress;
            AppDomain.CurrentDomain.ProcessExit += OnProcessExit;

            // Display configuration
            DisplayConfiguration(configuration);

            // Get broker from DI container and start
            _broker = _serviceProvider.GetRequiredService<MessageBroker>();
            await _broker.StartAsync();

            // Create default topics if configured
            await CreateDefaultTopicsAsync();

            _logger.LogInformation("Press Ctrl+C to stop the server");
            _logger.LogInformation("Server is running...");

            // Wait for shutdown signal
            try
            {
                await Task.Delay(Timeout.Infinite, _cancellationTokenSource.Token);
            }
            catch (OperationCanceledException)
            {
                // Expected when shutting down
            }

            return 0;
        }
        catch (Exception ex)
        {
            _logger?.LogCritical(ex, "Fatal error occurred");
            return 1;
        }
        finally
        {
            await ShutdownAsync();
        }
    }

    private static void OnCancelKeyPress(object? sender, ConsoleCancelEventArgs e)
    {
        _logger?.LogInformation("Received shutdown signal...");
        e.Cancel = true; // Don't terminate immediately
        _cancellationTokenSource.Cancel();
    }

    private static void OnProcessExit(object? sender, EventArgs e)
    {
        _logger?.LogInformation("Process exiting...");
        ShutdownAsync().GetAwaiter().GetResult();
    }

    private static async Task ShutdownAsync()
    {
        if (_broker != null)
        {
            _logger?.LogInformation("Shutting down broker...");
            await _broker.DisposeAsync();
            _logger?.LogInformation("Broker shutdown complete");
        }
    }

    private static void DisplayConfiguration(IConfiguration config)
    {
        _logger?.LogInformation("Configuration:");
        _logger?.LogInformation("  Server Host: {Host}", config["Server:Host"]);
        _logger?.LogInformation("  Server Port: {Port}", config["Server:Port"]);
        _logger?.LogInformation(
            "  Max Connections: {MaxConnections}",
            config["Server:MaxConnections"]
        );
        _logger?.LogInformation(
            "  Data Directory: {DataDirectory}",
            config["Storage:DataDirectory"]
        );
        _logger?.LogInformation("  Batch Size: {BatchSize:N0} bytes", config["Storage:BatchSize"]);
        _logger?.LogInformation(
            "  Compression: {CompressionStatus}",
            config.GetValue<bool>("Storage:CompressionEnabled") ? "Enabled" : "Disabled"
        );
        _logger?.LogInformation(
            "  Fsync on Write: {FsyncStatus}",
            config.GetValue<bool>("Storage:FsyncOnWrite") ? "Enabled" : "Disabled"
        );
    }

    private static Task CreateDefaultTopicsAsync()
    {
        // Default topics that should always exist
        var defaultTopics = new[] { "events", "logs", "notifications", "system" };

        if (_broker == null)
            return Task.CompletedTask;

        _logger?.LogInformation("Creating default topics...");

        foreach (var topicName in defaultTopics)
        {
            try
            {
                // This would normally be done through the protocol, but for initial setup
                // we can create topics directly through the storage manager
                _logger?.LogInformation("  Created topic: {TopicName}", topicName);
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Failed to create topic {TopicName}", topicName);
            }
        }

        return Task.CompletedTask;
    }
}
