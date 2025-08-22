using System.Text.Json;

namespace QueueServer.Core.Configuration;

/// <summary>
/// Server configuration settings
/// </summary>
public sealed record ServerConfiguration
{
    public string Host { get; init; } = "127.0.0.1";
    public int Port { get; init; } = 9999;
    public int MaxConnections { get; init; } = 100;
    public TimeSpan ConnectionTimeout { get; init; } = TimeSpan.FromMinutes(5);
    public TimeSpan HeartbeatInterval { get; init; } = TimeSpan.FromSeconds(30);
    public int ReceiveBufferSize { get; init; } = 64 * 1024; // 64KB
    public int SendBufferSize { get; init; } = 64 * 1024; // 64KB
}

/// <summary>
/// Storage configuration settings
/// </summary>
public sealed record StorageConfiguration
{
    public string DataDirectory { get; init; } = "./storage/data";
    public long MaxLogSize { get; init; } = 100 * 1024 * 1024; // 100MB
    public bool FsyncOnWrite { get; init; } = true;
    public int BatchSize { get; init; } = 64 * 1024; // 64KB
    public int BatchCount { get; init; } = 100;
    public TimeSpan BatchTimeout { get; init; } = TimeSpan.FromMilliseconds(100);
    public bool CompressionEnabled { get; init; } = true;
    public TimeSpan FsyncInterval { get; init; } = TimeSpan.FromSeconds(1);
}

/// <summary>
/// Topic configuration settings
/// </summary>
public sealed record TopicConfiguration
{
    public int DefaultPartitions { get; init; } = 1;
    public int MaxPartitions { get; init; } = 16;
    public TimeSpan RetentionPeriod { get; init; } = TimeSpan.FromHours(24);
    public int MaxMessageSize { get; init; } = 1024 * 1024; // 1MB
    public bool AutoCreateTopics { get; init; } = true;
}

/// <summary>
/// Client configuration settings
/// </summary>
public sealed record ClientConfiguration
{
    public TimeSpan HeartbeatInterval { get; init; } = TimeSpan.FromSeconds(30);
    public TimeSpan RequestTimeout { get; init; } = TimeSpan.FromSeconds(30);
    public TimeSpan ReconnectDelay { get; init; } = TimeSpan.FromSeconds(5);
    public int MaxRetryAttempts { get; init; } = 3;
    public int ReceiveBufferSize { get; init; } = 64 * 1024; // 64KB
    public int SendBufferSize { get; init; } = 64 * 1024; // 64KB
}

/// <summary>
/// Complete broker configuration
/// </summary>
public sealed record BrokerConfiguration
{
    public ServerConfiguration Server { get; init; } = new();
    public StorageConfiguration Storage { get; init; } = new();
    public TopicConfiguration Topics { get; init; } = new();
    public ClientConfiguration Client { get; init; } = new();
}

/// <summary>
/// Configuration manager supporting files and environment variables
/// </summary>
public sealed class ConfigurationManager
{
    private static readonly Lazy<ConfigurationManager> _instance = new(() =>
        new ConfigurationManager()
    );
    public static ConfigurationManager Instance => _instance.Value;

    private BrokerConfiguration _configuration;

    private ConfigurationManager()
    {
        _configuration = LoadConfiguration();
    }

    /// <summary>
    /// Get current configuration
    /// </summary>
    public BrokerConfiguration Configuration => _configuration;

    /// <summary>
    /// Reload configuration from sources
    /// </summary>
    public void Reload()
    {
        _configuration = LoadConfiguration();
    }

    private static BrokerConfiguration LoadConfiguration()
    {
        var config = new BrokerConfiguration();

        // Try to load from appsettings.json first
        const string configFile = "appsettings.json";
        if (File.Exists(configFile))
        {
            try
            {
                var json = File.ReadAllText(configFile);
                var fileConfig = JsonSerializer.Deserialize<BrokerConfiguration>(
                    json,
                    new JsonSerializerOptions
                    {
                        PropertyNameCaseInsensitive = true,
                        ReadCommentHandling = JsonCommentHandling.Skip,
                    }
                );

                if (fileConfig != null)
                {
                    config = fileConfig;
                }
            }
            catch (Exception ex)
            {
                // Note: Cannot use ILogger here as this is called during DI setup
                // Will be handled by the consuming application
                throw new InvalidOperationException(
                    $"Failed to load configuration from {configFile}: {ex.Message}", ex);
            }
        }

        // Override with environment variables
        config = OverrideWithEnvironmentVariables(config);

        return config;
    }

    private static BrokerConfiguration OverrideWithEnvironmentVariables(BrokerConfiguration config)
    {
        var serverConfig = config.Server;
        var storageConfig = config.Storage;
        var topicsConfig = config.Topics;
        var clientConfig = config.Client;

        // Server configuration
        if (Environment.GetEnvironmentVariable("QB_SERVER_HOST") is { } host)
            serverConfig = serverConfig with { Host = host };

        if (
            Environment.GetEnvironmentVariable("QB_SERVER_PORT") is { } portStr
            && int.TryParse(portStr, out var port)
        )
            serverConfig = serverConfig with { Port = port };

        if (
            Environment.GetEnvironmentVariable("QB_SERVER_MAX_CONNECTIONS") is { } maxConnStr
            && int.TryParse(maxConnStr, out var maxConn)
        )
            serverConfig = serverConfig with { MaxConnections = maxConn };

        // Storage configuration
        if (Environment.GetEnvironmentVariable("QB_STORAGE_DATA_DIR") is { } dataDir)
            storageConfig = storageConfig with { DataDirectory = dataDir };

        if (
            Environment.GetEnvironmentVariable("QB_STORAGE_MAX_LOG_SIZE") is { } maxLogSizeStr
            && long.TryParse(maxLogSizeStr, out var maxLogSize)
        )
            storageConfig = storageConfig with { MaxLogSize = maxLogSize };

        if (
            Environment.GetEnvironmentVariable("QB_STORAGE_BATCH_SIZE") is { } batchSizeStr
            && int.TryParse(batchSizeStr, out var batchSize)
        )
            storageConfig = storageConfig with { BatchSize = batchSize };

        if (
            Environment.GetEnvironmentVariable("QB_STORAGE_COMPRESSION") is { } compressionStr
            && bool.TryParse(compressionStr, out var compression)
        )
            storageConfig = storageConfig with { CompressionEnabled = compression };

        // Topic configuration
        if (
            Environment.GetEnvironmentVariable("QB_TOPICS_DEFAULT_PARTITIONS") is { } defaultPartStr
            && int.TryParse(defaultPartStr, out var defaultPart)
        )
            topicsConfig = topicsConfig with { DefaultPartitions = defaultPart };

        if (
            Environment.GetEnvironmentVariable("QB_TOPICS_RETENTION_HOURS") is { } retentionStr
            && int.TryParse(retentionStr, out var retentionHours)
        )
            topicsConfig = topicsConfig with
            {
                RetentionPeriod = TimeSpan.FromHours(retentionHours),
            };

        return config with
        {
            Server = serverConfig,
            Storage = storageConfig,
            Topics = topicsConfig,
            Client = clientConfig,
        };
    }
}
