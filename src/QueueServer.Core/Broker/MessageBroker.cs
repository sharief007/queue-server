using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using QueueServer.Core.Configuration;
using QueueServer.Core.Models;
using QueueServer.Core.Network;
using QueueServer.Core.Protocol;
using QueueServer.Core.Storage;
using QueueServer.Core.Subscriptions;

namespace QueueServer.Core.Broker;

/// <summary>
/// Main message broker orchestrating all components
/// </summary>
public sealed class MessageBroker : IAsyncDisposable
{
    private readonly BrokerConfiguration _config;
    private readonly ILogger<MessageBroker> _logger;
    private readonly SequentialStorageManager _storageManager;
    private readonly SubscriptionManager _subscriptionManager;
    private readonly ProtocolHandler _protocolHandler;
    private readonly TcpServer _tcpServer;

    // Connection tracking
    private readonly ConcurrentDictionary<string, ClientConnection> _connections = new();
    private readonly ConcurrentDictionary<string, string> _connectionToSubscriber = new();

    // Background tasks
    private readonly CancellationTokenSource _cancellationTokenSource = new();
    private readonly List<Task> _backgroundTasks = new();

    private volatile bool _isRunning;
    private volatile bool _disposed;

    public MessageBroker(
        ILogger<MessageBroker> logger,
        BrokerConfiguration? config = null)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _config = config ?? ConfigurationManager.Instance.Configuration;

        // Initialize components
        _storageManager = new SequentialStorageManager(_config.Storage);
        _subscriptionManager = new SubscriptionManager();
        _protocolHandler = new ProtocolHandler(_storageManager, _subscriptionManager);
        _tcpServer = new TcpServer(_config.Server);

        // Wire up events
        _tcpServer.ClientConnected += OnClientConnected;
        _tcpServer.ClientDisconnected += OnClientDisconnected;

        // Subscribe to message delivery events
        WireUpMessageDelivery();
    }

    public bool IsRunning => _isRunning;
    public int ConnectionCount => _connections.Count;
    public BrokerConfiguration Configuration => _config;

    /// <summary>
    /// Start the message broker
    /// </summary>
    public async Task StartAsync()
    {
        if (_isRunning) return;

        _logger.LogInformation("Starting Message Broker...");

        // Start TCP server
        await _tcpServer.StartAsync();

        // Start background tasks
        StartBackgroundTasks();

        _isRunning = true;

        _logger.LogInformation("Message Broker started successfully");
        _logger.LogInformation("- Server: {Host}:{Port}", _config.Server.Host, _config.Server.Port);
        _logger.LogInformation("- Storage: {DataDirectory}", _config.Storage.DataDirectory);
        _logger.LogInformation("- Max Connections: {MaxConnections}", _config.Server.MaxConnections);
    }

    /// <summary>
    /// Stop the message broker
    /// </summary>
    public async Task StopAsync()
    {
        if (!_isRunning) return;

        _logger.LogInformation("Stopping Message Broker...");

        _isRunning = false;
        _cancellationTokenSource.Cancel();

        // Stop TCP server
        await _tcpServer.StopAsync();

        // Wait for background tasks to complete
        try
        {
            await Task.WhenAll(_backgroundTasks);
        }
        catch (OperationCanceledException)
        {
            // Expected
        }

        // Clear connections
        _connections.Clear();
        _connectionToSubscriber.Clear();

        _logger.LogInformation("Message Broker stopped");
    }

    /// <summary>
    /// Get broker statistics
    /// </summary>
    public BrokerStatistics GetStatistics()
    {
        var topicStats = _storageManager.ListTopics()
            .Select(topicName =>
            {
                var topic = _storageManager.GetOrCreateTopic(topicName);
                return new TopicStatistics(topicName, topic.MessageCount, topic.TotalBytes);
            })
            .ToList();

        var subscriptionStats = _subscriptionManager.GetAllSubscriptions()
            .Select(sub => new SubscriptionStatistics(
                sub.SubscriptionId,
                sub.TopicName,
                sub.CurrentOffset,
                sub.SubscriberCount,
                sub.MessagesDelivered,
                sub.MessagesProcessed))
            .ToList();

        return new BrokerStatistics(
            ConnectionCount,
            topicStats.Count,
            subscriptionStats.Count,
            topicStats,
            subscriptionStats);
    }

    /// <summary>
    /// Handle new client connection
    /// </summary>
    private Task OnClientConnected(ClientConnection connection)
    {
        _connections[connection.ConnectionId] = connection;

        _logger.LogInformation("Client connected: {RemoteEndPoint} ({ConnectionId})",
            connection.RemoteEndPoint, connection.ConnectionId);

        // Start handling messages from this client
        _ = Task.Run(() => HandleClientMessages(connection), _cancellationTokenSource.Token);
        return Task.CompletedTask;
    }

    /// <summary>
    /// Handle client disconnection
    /// </summary>
    private Task OnClientDisconnected(ClientConnection connection)
    {
        _connections.TryRemove(connection.ConnectionId, out _);

        // Remove from any subscriptions
        if (_connectionToSubscriber.TryRemove(connection.ConnectionId, out var subscriberId))
        {
            _subscriptionManager.RemoveSubscriber(subscriberId);
        }

        _logger.LogInformation("Client disconnected: {RemoteEndPoint} ({ConnectionId})",
            connection.RemoteEndPoint, connection.ConnectionId);
        return Task.CompletedTask;
    }

    /// <summary>
    /// Handle messages from a client connection
    /// </summary>
    private async Task HandleClientMessages(ClientConnection connection)
    {
        try
        {
            while (connection.IsConnected && _isRunning && !_cancellationTokenSource.Token.IsCancellationRequested)
            {
                var (message, success) = await connection.ReceiveMessageAsync(_cancellationTokenSource.Token);

                if (!success || message == null)
                {
                    break;
                }

                // Create protocol request
                var request = new ProtocolRequest(connection, message.Value, Guid.NewGuid().ToString());

                // Handle the request
                var response = await _protocolHandler.HandleMessageAsync(request);

                // Send response if needed
                if (response.Success)
                {
                    await connection.SendMessageAsync(response.Message, _cancellationTokenSource.Token);
                }
                else
                {
                    _logger.LogWarning("Protocol error for connection {ConnectionId}: {ErrorMessage}",
                        connection.ConnectionId, response.ErrorMessage);
                    await connection.SendMessageAsync(response.Message, _cancellationTokenSource.Token);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected when shutting down
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error handling client {ConnectionId}", connection.ConnectionId);
        }
        finally
        {
            await connection.DisconnectAsync();
        }
    }

    /// <summary>
    /// Start background maintenance tasks
    /// </summary>
    private void StartBackgroundTasks()
    {
        // Heartbeat monitor task
        _backgroundTasks.Add(Task.Run(HeartbeatMonitor, _cancellationTokenSource.Token));

        // Statistics reporting task
        _backgroundTasks.Add(Task.Run(StatisticsReporter, _cancellationTokenSource.Token));
    }

    /// <summary>
    /// Monitor client heartbeats and disconnect inactive clients
    /// </summary>
    private async Task HeartbeatMonitor()
    {
        while (_isRunning && !_cancellationTokenSource.Token.IsCancellationRequested)
        {
            try
            {
                var unhealthyConnections = new List<string>();

                foreach (var (connectionId, connection) in _connections)
                {
                    if (!connection.IsHealthy())
                    {
                        unhealthyConnections.Add(connectionId);
                    }
                }

                foreach (var connectionId in unhealthyConnections)
                {
                    _logger.LogInformation("Disconnecting unhealthy client: {ConnectionId}", connectionId);
                    await _tcpServer.RemoveConnectionAsync(connectionId);
                }

                await Task.Delay(_config.Server.HeartbeatInterval, _cancellationTokenSource.Token);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in heartbeat monitor");
            }
        }
    }

    /// <summary>
    /// Report periodic statistics
    /// </summary>
    private async Task StatisticsReporter()
    {
        while (_isRunning && !_cancellationTokenSource.Token.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(TimeSpan.FromMinutes(1), _cancellationTokenSource.Token);

                var stats = GetStatistics();
                _logger.LogInformation("Broker Stats - Connections: {ActiveConnections}, Topics: {TopicCount}, Subscriptions: {SubscriptionCount}",
                    stats.ActiveConnections, stats.TopicCount, stats.SubscriptionCount);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in statistics reporter");
            }
        }
    }

    /// <summary>
    /// Wire up message delivery from subscriptions to clients
    /// </summary>
    private void WireUpMessageDelivery()
    {
        // This would be called when subscriptions are created
        // For now, we'll handle it in the subscription events
    }

    /// <summary>
    /// Deliver message to a specific subscriber
    /// </summary>
    private async Task DeliverMessageToSubscriber(string subscriberId, Message message, string subscriptionId)
    {
        // Find the connection for this subscriber
        var connection = _connections.Values.FirstOrDefault(c => c.ConnectionId == subscriberId);
        if (connection?.IsConnected == true)
        {
            try
            {
                // Create data message for delivery
                var deliveryMessage = new MessageBuilder(MessageType.Data)
                    .WithSequenceNumber(message.SequenceNumber)
                    .WithProperty("subscription_id", subscriptionId)
                    .WithBody(message.Body.Span)
                    .Build();

                await connection.SendMessageAsync(deliveryMessage, _cancellationTokenSource.Token);

                // Update subscriber activity
                _subscriptionManager.UpdateSubscriberActivity(subscriberId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error delivering message to subscriber {SubscriberId}", subscriberId);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        await StopAsync();

        await _tcpServer.DisposeAsync();
        await _subscriptionManager.DisposeAsync();
        _storageManager.Dispose();
        _cancellationTokenSource.Dispose();
    }
}

/// <summary>
/// Broker statistics
/// </summary>
public sealed record BrokerStatistics(
    int ActiveConnections,
    int TopicCount,
    int SubscriptionCount,
    IReadOnlyList<TopicStatistics> Topics,
    IReadOnlyList<SubscriptionStatistics> Subscriptions);

/// <summary>
/// Topic statistics
/// </summary>
public sealed record TopicStatistics(
    string Name,
    ulong MessageCount,
    long TotalBytes);

/// <summary>
/// Subscription statistics
/// </summary>
public sealed record SubscriptionStatistics(
    string SubscriptionId,
    string TopicName,
    ulong CurrentOffset,
    int SubscriberCount,
    long MessagesDelivered,
    long MessagesProcessed);
