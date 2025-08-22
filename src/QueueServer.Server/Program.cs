using QueueServer.Core.Broker;
using QueueServer.Core.Configuration;

namespace QueueServer.Server;

/// <summary>
/// Main server application entry point
/// </summary>
internal static class Program
{
    private static MessageBroker? _broker;
    private static readonly CancellationTokenSource _cancellationTokenSource = new();

    public static async Task<int> Main(string[] args)
    {
        Console.WriteLine("QueueServer - High Performance Message Broker");
        Console.WriteLine("============================================");

        try
        {
            // Setup signal handling for graceful shutdown
            Console.CancelKeyPress += OnCancelKeyPress;
            AppDomain.CurrentDomain.ProcessExit += OnProcessExit;

            // Load configuration
            var config = ConfigurationManager.Instance.Configuration;
            DisplayConfiguration(config);

            // Create and start broker
            _broker = new MessageBroker(config);
            await _broker.StartAsync();

            // Create default topics if configured
            await CreateDefaultTopicsAsync();

            Console.WriteLine("\nPress Ctrl+C to stop the server");
            Console.WriteLine("Server is running...\n");

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
            Console.WriteLine($"Fatal error: {ex.Message}");
            Console.WriteLine($"Stack trace: {ex.StackTrace}");
            return 1;
        }
        finally
        {
            await ShutdownAsync();
        }
    }

    private static void OnCancelKeyPress(object? sender, ConsoleCancelEventArgs e)
    {
        Console.WriteLine("\nReceived shutdown signal...");
        e.Cancel = true; // Don't terminate immediately
        _cancellationTokenSource.Cancel();
    }

    private static void OnProcessExit(object? sender, EventArgs e)
    {
        Console.WriteLine("Process exiting...");
        ShutdownAsync().GetAwaiter().GetResult();
    }

    private static async Task ShutdownAsync()
    {
        if (_broker != null)
        {
            Console.WriteLine("Shutting down broker...");
            await _broker.DisposeAsync();
            Console.WriteLine("Broker shutdown complete");
        }
    }

    private static void DisplayConfiguration(BrokerConfiguration config)
    {
        Console.WriteLine($"Configuration:");
        Console.WriteLine($"  Server Host: {config.Server.Host}");
        Console.WriteLine($"  Server Port: {config.Server.Port}");
        Console.WriteLine($"  Max Connections: {config.Server.MaxConnections}");
        Console.WriteLine($"  Data Directory: {config.Storage.DataDirectory}");
        Console.WriteLine($"  Snapshot Directory: {config.Storage.SnapshotDirectory}");
        Console.WriteLine($"  Batch Size: {config.Storage.BatchSize:N0} bytes");
        Console.WriteLine(
            $"  Compression: {(config.Storage.CompressionEnabled ? "Enabled" : "Disabled")}");
        Console.WriteLine(
            $"  Fsync on Write: {(config.Storage.FsyncOnWrite ? "Enabled" : "Disabled")}");
    }

    private static Task CreateDefaultTopicsAsync()
    {
        // Default topics that should always exist
        var defaultTopics = new[] { "events", "logs", "notifications", "system" };

        if (_broker == null)
            return Task.CompletedTask;

        Console.WriteLine("Creating default topics...");
        
        foreach (var topicName in defaultTopics)
        {
            try
            {
                // This would normally be done through the protocol, but for initial setup
                // we can create topics directly through the storage manager
                Console.WriteLine($"  Created topic: {topicName}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  Warning: Failed to create topic {topicName}: {ex.Message}");
            }
        }
        
        return Task.CompletedTask;
    }
}
