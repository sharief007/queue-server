using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using QueueServer.Core.Configuration;
using QueueServer.Core.Models;

namespace QueueServer.Client;

/// <summary>
/// High-performance client for QueueServer message broker
/// </summary>
public sealed class BrokerClient : IAsyncDisposable
{
    private readonly ILogger<BrokerClient>? _logger;
    private readonly ClientConfiguration _config;
    private readonly string _clientId;
    private Socket? _socket;
    private readonly IPEndPoint _serverEndPoint;
    private readonly CancellationTokenSource _cancellationTokenSource = new();

    // Connection state
    private ConnectionState _state = ConnectionState.Disconnected;
    private DateTime _lastHeartbeat = DateTime.UtcNow;
    private ulong _sequenceNumber;

    // Message handling
    private readonly ConcurrentDictionary<ulong, TaskCompletionSource<Message>> _pendingRequests =
        new();
    private readonly ConcurrentDictionary<string, Func<Message, Task>> _subscriptionHandlers =
        new();

    // Background tasks
    private Task? _receiveTask;
    private Task? _heartbeatTask;

    // Thread safety
    private readonly SemaphoreSlim _sendSemaphore = new(1, 1);
    private readonly object _stateLock = new();

    private volatile bool _disposed;

    public BrokerClient(
        string host = "127.0.0.1",
        int port = 9999,
        string? clientId = null,
        ClientConfiguration? config = null,
        ILogger<BrokerClient>? logger = null
    )
    {
        _logger = logger;
        _clientId = clientId ?? Guid.NewGuid().ToString();
        _config = config ?? new ClientConfiguration();
        _serverEndPoint = new IPEndPoint(IPAddress.Parse(host), port);
    }

    public string ClientId => _clientId;
    public ConnectionState State => _state;
    public bool IsConnected => _state == ConnectionState.Connected && _socket?.Connected == true;

    /// <summary>
    /// Connect to the broker
    /// </summary>
    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        lock (_stateLock)
        {
            if (_state != ConnectionState.Disconnected)
                throw new InvalidOperationException($"Cannot connect when state is {_state}");

            _state = ConnectionState.Connecting;
        }

        try
        {
            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            _socket.NoDelay = true;
            _socket.ReceiveBufferSize = _config.ReceiveBufferSize;
            _socket.SendBufferSize = _config.SendBufferSize;

            await _socket.ConnectAsync(_serverEndPoint, cancellationToken);

            lock (_stateLock)
            {
                _state = ConnectionState.Connected;
            }

            // Start background tasks
            _receiveTask = Task.Run(ReceiveWorker, _cancellationTokenSource.Token);
            _heartbeatTask = Task.Run(HeartbeatWorker, _cancellationTokenSource.Token);

            _lastHeartbeat = DateTime.UtcNow;

            _logger?.LogInformation("Connected to broker at {ServerEndPoint}", _serverEndPoint);
        }
        catch
        {
            lock (_stateLock)
            {
                _state = ConnectionState.Disconnected;
            }
            _socket?.Dispose();
            _socket = null;
            throw;
        }
    }

    /// <summary>
    /// Disconnect from the broker
    /// </summary>
    public async Task DisconnectAsync()
    {
        if (_state == ConnectionState.Disconnected)
            return;

        lock (_stateLock)
        {
            _state = ConnectionState.Disconnecting;
        }

        _cancellationTokenSource.Cancel();

        // Wait for background tasks to complete
        try
        {
            if (_receiveTask != null)
                await _receiveTask;
            if (_heartbeatTask != null)
                await _heartbeatTask;
        }
        catch (OperationCanceledException)
        {
            // Expected
        }

        // Close socket
        try
        {
            if (_socket?.Connected == true)
            {
                _socket.Shutdown(SocketShutdown.Both);
            }
        }
        catch
        {
            // Ignore errors during shutdown
        }
        finally
        {
            _socket?.Dispose();
            _socket = null;
        }

        // Cancel pending requests
        foreach (var pending in _pendingRequests.Values)
        {
            pending.TrySetCanceled();
        }
        _pendingRequests.Clear();

        lock (_stateLock)
        {
            _state = ConnectionState.Disconnected;
        }

        _logger?.LogInformation("Disconnected from broker");
    }

    /// <summary>
    /// Create a topic
    /// </summary>
    public async Task<bool> CreateTopicAsync(
        string topicName,
        int retentionHours = 24,
        CancellationToken cancellationToken = default
    )
    {
        var message = new MessageBuilder(MessageType.CreateTopic)
            .WithSequenceNumber(GetNextSequenceNumber())
            .WithProperty("topic", topicName)
            .WithProperty("retention_hours", retentionHours.ToString())
            .Build();

        var response = await SendAndWaitAsync(message, cancellationToken);
        return IsSuccessResponse(response);
    }

    /// <summary>
    /// Create a subscription
    /// </summary>
    public async Task<bool> CreateSubscriptionAsync(
        string subscriptionId,
        string topicName,
        ulong startOffset = 0,
        CancellationToken cancellationToken = default
    )
    {
        var message = new MessageBuilder(MessageType.CreateSubscription)
            .WithSequenceNumber(GetNextSequenceNumber())
            .WithProperty("subscription_id", subscriptionId)
            .WithProperty("topic", topicName)
            .WithProperty("start_offset", startOffset.ToString())
            .Build();

        var response = await SendAndWaitAsync(message, cancellationToken);
        return IsSuccessResponse(response);
    }

    /// <summary>
    /// Delete a subscription
    /// </summary>
    public async Task<bool> DeleteSubscriptionAsync(
        string subscriptionId,
        CancellationToken cancellationToken = default
    )
    {
        var message = new MessageBuilder(MessageType.DeleteSubscription)
            .WithSequenceNumber(GetNextSequenceNumber())
            .WithProperty("subscription_id", subscriptionId)
            .Build();

        var response = await SendAndWaitAsync(message, cancellationToken);
        return IsSuccessResponse(response);
    }

    /// <summary>
    /// Publish a message to a topic
    /// </summary>
    public async Task<bool> PublishAsync(
        string topicName,
        ReadOnlyMemory<byte> data,
        Dictionary<string, string>? properties = null,
        CancellationToken cancellationToken = default
    )
    {
        var builder = new MessageBuilder(MessageType.Publish)
            .WithSequenceNumber(GetNextSequenceNumber())
            .WithProperty("topic", topicName)
            .WithBody(data.Span);

        if (properties != null)
        {
            foreach (var (key, value) in properties)
            {
                builder.WithProperty(key, value);
            }
        }

        var message = builder.Build();
        var response = await SendAndWaitAsync(message, cancellationToken);
        return IsSuccessResponse(response);
    }

    /// <summary>
    /// Publish a text message to a topic
    /// </summary>
    public async Task<bool> PublishTextAsync(
        string topicName,
        string text,
        Dictionary<string, string>? properties = null,
        CancellationToken cancellationToken = default
    )
    {
        var data = System.Text.Encoding.UTF8.GetBytes(text);
        return await PublishAsync(topicName, data, properties, cancellationToken);
    }

    /// <summary>
    /// Subscribe to a subscription
    /// </summary>
    public async Task<bool> SubscribeAsync(
        string subscriptionId,
        Func<Message, Task> messageHandler,
        CancellationToken cancellationToken = default
    )
    {
        var message = new MessageBuilder(MessageType.Subscribe)
            .WithSequenceNumber(GetNextSequenceNumber())
            .WithProperty("subscription_id", subscriptionId)
            .Build();

        var response = await SendAndWaitAsync(message, cancellationToken);
        if (IsSuccessResponse(response))
        {
            _subscriptionHandlers[subscriptionId] = messageHandler;
            return true;
        }

        return false;
    }

    /// <summary>
    /// Unsubscribe from a subscription
    /// </summary>
    public async Task<bool> UnsubscribeAsync(
        string subscriptionId,
        CancellationToken cancellationToken = default
    )
    {
        var message = new MessageBuilder(MessageType.Unsubscribe)
            .WithSequenceNumber(GetNextSequenceNumber())
            .WithProperty("subscription_id", subscriptionId)
            .Build();

        var response = await SendAndWaitAsync(message, cancellationToken);
        if (IsSuccessResponse(response))
        {
            _subscriptionHandlers.TryRemove(subscriptionId, out _);
            return true;
        }

        return false;
    }

    /// <summary>
    /// Send message and wait for response
    /// </summary>
    private async Task<Message?> SendAndWaitAsync(
        Message message,
        CancellationToken cancellationToken = default
    )
    {
        if (!IsConnected)
            throw new InvalidOperationException("Not connected to broker");

        var tcs = new TaskCompletionSource<Message>();
        _pendingRequests[message.SequenceNumber] = tcs;

        try
        {
            await SendMessageAsync(message, cancellationToken);

            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken
            );
            timeoutCts.CancelAfter(_config.RequestTimeout);

            return await tcs.Task.WaitAsync(timeoutCts.Token);
        }
        catch (OperationCanceledException)
        {
            return null;
        }
        finally
        {
            _pendingRequests.TryRemove(message.SequenceNumber, out _);
        }
    }

    /// <summary>
    /// Send message to broker
    /// </summary>
    private async Task SendMessageAsync(
        Message message,
        CancellationToken cancellationToken = default
    )
    {
        if (_socket == null || !IsConnected)
            throw new InvalidOperationException("Not connected to broker");

        await _sendSemaphore.WaitAsync(cancellationToken);
        try
        {
            var buffer = new byte[message.TotalSize];
            message.Serialize(buffer);

            var sent = await _socket.SendAsync(buffer, SocketFlags.None, cancellationToken);
            if (sent != buffer.Length)
            {
                throw new InvalidOperationException("Failed to send complete message");
            }
        }
        finally
        {
            _sendSemaphore.Release();
        }
    }

    /// <summary>
    /// Background worker for receiving messages
    /// </summary>
    private async Task ReceiveWorker()
    {
        try
        {
            while (IsConnected && !_cancellationTokenSource.Token.IsCancellationRequested)
            {
                var message = await ReceiveMessageAsync(_cancellationTokenSource.Token);
                if (message != null)
                {
                    await HandleReceivedMessage(message.Value);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected when shutting down
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error in receive worker");
        }
    }

    /// <summary>
    /// Receive a message from the broker
    /// </summary>
    private async Task<Message?> ReceiveMessageAsync(CancellationToken cancellationToken)
    {
        if (_socket == null || !IsConnected)
            return null;

        try
        {
            // Read header
            var headerBuffer = new byte[Message.HeaderSize];
            var received = await _socket.ReceiveAsync(
                headerBuffer,
                SocketFlags.None,
                cancellationToken
            );
            if (received != Message.HeaderSize)
                return null;

            // Get total length
            var totalLength = BitConverter.ToInt32(headerBuffer, 0);
            if (totalLength < Message.HeaderSize)
                return null;

            // Read complete message
            var messageBuffer = new byte[totalLength];
            Array.Copy(headerBuffer, messageBuffer, Message.HeaderSize);

            var remaining = totalLength - Message.HeaderSize;
            if (remaining > 0)
            {
                var remainingReceived = await _socket.ReceiveAsync(
                    messageBuffer.AsMemory(Message.HeaderSize, remaining),
                    SocketFlags.None,
                    cancellationToken
                );
                if (remainingReceived != remaining)
                    return null;
            }

            var (message, _) = Message.Deserialize(messageBuffer);
            return message;
        }
        catch (Exception)
        {
            return null;
        }
    }

    /// <summary>
    /// Handle received message
    /// </summary>
    private async Task HandleReceivedMessage(Message message)
    {
        // Check if it's a response to a pending request
        if (_pendingRequests.TryGetValue(message.SequenceNumber, out var tcs))
        {
            tcs.TrySetResult(message);
            return;
        }

        // Handle heartbeat
        if (message.MessageType == MessageType.Heartbeat)
        {
            _lastHeartbeat = DateTime.UtcNow;
            return;
        }

        // Handle subscription data
        if (message.MessageType == MessageType.Data)
        {
            var subscriptionId = GetProperty(message, "subscription_id");
            if (
                !string.IsNullOrEmpty(subscriptionId)
                && _subscriptionHandlers.TryGetValue(subscriptionId, out var handler)
            )
            {
                try
                {
                    await handler(message);
                }
                catch (Exception ex)
                {
                    _logger?.LogError(
                        ex,
                        "Error in subscription handler for subscription {SubscriptionId}",
                        subscriptionId
                    );
                }
            }
        }
    }

    /// <summary>
    /// Background worker for sending heartbeats
    /// </summary>
    private async Task HeartbeatWorker()
    {
        try
        {
            while (IsConnected && !_cancellationTokenSource.Token.IsCancellationRequested)
            {
                await Task.Delay(_config.HeartbeatInterval, _cancellationTokenSource.Token);

                if (IsConnected)
                {
                    var heartbeat = new MessageBuilder(MessageType.Heartbeat)
                        .WithSequenceNumber(GetNextSequenceNumber())
                        .Build();

                    try
                    {
                        await SendMessageAsync(heartbeat, _cancellationTokenSource.Token);
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogError(ex, "Error sending heartbeat");
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected when shutting down
        }
    }

    /// <summary>
    /// Get next sequence number
    /// </summary>
    private ulong GetNextSequenceNumber()
    {
        return Interlocked.Increment(ref _sequenceNumber);
    }

    /// <summary>
    /// Check if response indicates success
    /// </summary>
    private static bool IsSuccessResponse(Message? response)
    {
        return response.HasValue && GetProperty(response.Value, "status") == "success";
    }

    /// <summary>
    /// Extract property from message
    /// </summary>
    private static string? GetProperty(Message message, string key)
    {
        // Parse properties from binary format
        var properties = ParseProperties(message.Properties.Span);
        return properties.GetValueOrDefault(key);
    }

    /// <summary>
    /// Parse properties from binary format
    /// </summary>
    private static Dictionary<string, string> ParseProperties(ReadOnlySpan<byte> data)
    {
        var properties = new Dictionary<string, string>();

        if (data.Length == 0)
            return properties;

        var position = 0;
        while (position < data.Length)
        {
            var start = position;
            while (position < data.Length && data[position] != 0)
            {
                position++;
            }

            if (position >= data.Length)
                break;

            var propertyData = data[start..position];
            var equalPos = propertyData.IndexOf((byte)'=');

            if (equalPos > 0 && equalPos < propertyData.Length - 1)
            {
                var key = System.Text.Encoding.UTF8.GetString(propertyData[..equalPos]);
                var value = System.Text.Encoding.UTF8.GetString(propertyData[(equalPos + 1)..]);
                properties[key] = value;
            }

            position++; // Skip null terminator
        }

        return properties;
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;
        _disposed = true;

        await DisconnectAsync();
        _cancellationTokenSource.Dispose();
        _sendSemaphore.Dispose();
    }
}
