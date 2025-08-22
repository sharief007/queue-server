namespace QueueServer.Core.Configuration;

/// <summary>
/// Server configuration options
/// </summary>
public sealed class ServerOptions
{
    public const string SectionName = "Server";

    public string Host { get; set; } = "127.0.0.1";
    public int Port { get; set; } = 9999;
    public int MaxConnections { get; set; } = 100;
    public TimeSpan ConnectionTimeout { get; set; } = TimeSpan.FromMinutes(5);
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(30);
    public int ReceiveBufferSize { get; set; } = 64 * 1024; // 64KB
    public int SendBufferSize { get; set; } = 64 * 1024; // 64KB
}

/// <summary>
/// Storage configuration options
/// </summary>
public sealed class StorageOptions
{
    public const string SectionName = "Storage";

    public string DataDirectory { get; set; } = "./storage/data";
    public long MaxLogSize { get; set; } = 100 * 1024 * 1024; // 100MB
    public bool FsyncOnWrite { get; set; } = true;
    public int BatchSize { get; set; } = 64 * 1024; // 64KB
    public int BatchCount { get; set; } = 100;
    public TimeSpan BatchTimeout { get; set; } = TimeSpan.FromMilliseconds(100);
    public bool CompressionEnabled { get; set; } = true;
    public TimeSpan FsyncInterval { get; set; } = TimeSpan.FromSeconds(1);
}

/// <summary>
/// Topic configuration options
/// </summary>
public sealed class TopicOptions
{
    public const string SectionName = "Topics";

    public int DefaultPartitions { get; set; } = 1;
    public int MaxPartitions { get; set; } = 16;
    public TimeSpan RetentionPeriod { get; set; } = TimeSpan.FromHours(24);
    public int MaxMessageSize { get; set; } = 1024 * 1024; // 1MB
    public bool AutoCreateTopics { get; set; } = true;
}

/// <summary>
/// Client configuration options
/// </summary>
public sealed class ClientOptions
{
    public const string SectionName = "Client";

    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(30);
    public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(30);
    public TimeSpan ReconnectDelay { get; set; } = TimeSpan.FromSeconds(5);
    public int MaxRetryAttempts { get; set; } = 3;
    public int ReceiveBufferSize { get; set; } = 64 * 1024; // 64KB
    public int SendBufferSize { get; set; } = 64 * 1024; // 64KB
}
