#pragma warning disable CS1998 // Async method lacks 'await' operators
using System.Collections.Concurrent;
using System.Threading.Channels;
using QueueServer.Core.Models;
using QueueServer.Core.Storage;

namespace QueueServer.Core.Subscriptions;

/// <summary>
/// Subscriber information within a subscription
/// </summary>
public sealed record SubscriberInfo(string SubscriberId, string ConnectionId, DateTime LastActivity)
{
    public SubscriberInfo UpdateActivity() => this with { LastActivity = DateTime.UtcNow };
}

/// <summary>
/// Subscription (consumer group) with round-robin delivery
/// </summary>
public sealed class Subscription : IAsyncDisposable
{
    private readonly string _subscriptionId;
    private readonly string _topicName;
    private readonly SubscriptionReader _reader;
    private readonly ConcurrentDictionary<string, SubscriberInfo> _subscribers = new();
    private readonly Channel<Message> _messageChannel;
    private readonly ChannelWriter<Message> _messageWriter;
    private readonly ChannelReader<Message> _messageReader;
    private readonly CancellationTokenSource _cancellationTokenSource = new();
    private readonly Task _deliveryTask;

    // Round-robin state
    private readonly object _roundRobinLock = new();
    private string[] _subscriberIds = Array.Empty<string>();
    private int _roundRobinIndex;

    // Statistics
    private long _messagesDelivered;
    private long _messagesProcessed;

    private volatile bool _disposed;

    public Subscription(
        string subscriptionId,
        string topicName,
        SubscriptionReader reader,
        int channelCapacity = 1000
    )
    {
        _subscriptionId = subscriptionId;
        _topicName = topicName;
        _reader = reader;

        // Create bounded channel for message delivery
        var channelOptions = new BoundedChannelOptions(channelCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = true,
        };

        _messageChannel = Channel.CreateBounded<Message>(channelOptions);
        _messageWriter = _messageChannel.Writer;
        _messageReader = _messageChannel.Reader;

        // Start message delivery task
        _deliveryTask = Task.Run(DeliveryWorker, _cancellationTokenSource.Token);
    }

    public string SubscriptionId => _subscriptionId;
    public string TopicName => _topicName;
    public ulong CurrentOffset => _reader.CurrentOffset;
    public int SubscriberCount => _subscribers.Count;
    public long MessagesDelivered => _messagesDelivered;
    public long MessagesProcessed => _messagesProcessed;

    /// <summary>
    /// Add a subscriber to this subscription
    /// </summary>
    public bool AddSubscriber(string subscriberId, string connectionId)
    {
        var subscriber = new SubscriberInfo(subscriberId, connectionId, DateTime.UtcNow);
        var added = _subscribers.TryAdd(subscriberId, subscriber);

        if (added)
        {
            UpdateRoundRobinArray();
        }

        return added;
    }

    /// <summary>
    /// Remove a subscriber from this subscription
    /// </summary>
    public bool RemoveSubscriber(string subscriberId)
    {
        var removed = _subscribers.TryRemove(subscriberId, out _);

        if (removed)
        {
            UpdateRoundRobinArray();
        }

        return removed;
    }

    /// <summary>
    /// Update subscriber activity
    /// </summary>
    public void UpdateSubscriberActivity(string subscriberId)
    {
        _subscribers.AddOrUpdate(
            subscriberId,
            _ => throw new InvalidOperationException("Subscriber not found"),
            (_, info) => info.UpdateActivity()
        );
    }

    /// <summary>
    /// Get all subscribers
    /// </summary>
    public IReadOnlyCollection<SubscriberInfo> GetSubscribers()
    {
        return _subscribers.Values.ToList();
    }

    /// <summary>
    /// Enqueue a message for delivery
    /// </summary>
    public async Task<bool> EnqueueMessageAsync(
        Message message,
        CancellationToken cancellationToken = default
    )
    {
        if (_disposed)
            return false;

        try
        {
            await _messageWriter.WriteAsync(message, cancellationToken);
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    /// <summary>
    /// Get next subscriber in round-robin order
    /// </summary>
    public string? GetNextSubscriber()
    {
        lock (_roundRobinLock)
        {
            if (_subscriberIds.Length == 0)
                return null;

            var subscriber = _subscriberIds[_roundRobinIndex];
            _roundRobinIndex = (_roundRobinIndex + 1) % _subscriberIds.Length;
            return subscriber;
        }
    }

    /// <summary>
    /// Seek to a specific offset
    /// </summary>
    public void SeekToOffset(ulong offset)
    {
        _reader.SeekToOffset(offset);
    }

    /// <summary>
    /// Update round-robin array when subscribers change
    /// </summary>
    private void UpdateRoundRobinArray()
    {
        lock (_roundRobinLock)
        {
            _subscriberIds = _subscribers.Keys.ToArray();
            _roundRobinIndex = 0;
        }
    }

    /// <summary>
    /// Background worker for message delivery
    /// </summary>
    private async Task DeliveryWorker()
    {
        try
        {
            await foreach (
                var message in _messageReader.ReadAllAsync(_cancellationTokenSource.Token)
            )
            {
                var delivered = false;
                var attempts = 0;
                var maxAttempts = Math.Max(1, _subscribers.Count);

                while (
                    !delivered
                    && attempts < maxAttempts
                    && !_cancellationTokenSource.Token.IsCancellationRequested
                )
                {
                    var subscriberId = GetNextSubscriber();
                    if (subscriberId == null)
                    {
                        // No subscribers, break and message will be lost
                        break;
                    }

                    // Try to deliver to this subscriber
                    if (await TryDeliverMessage(subscriberId, message))
                    {
                        delivered = true;
                        Interlocked.Increment(ref _messagesDelivered);
                    }

                    attempts++;
                }

                Interlocked.Increment(ref _messagesProcessed);
            }
        }
        catch (OperationCanceledException)
        {
            // Expected when shutting down
        }
    }

    /// <summary>
    /// Try to deliver message to a specific subscriber
    /// </summary>
    private async Task<bool> TryDeliverMessage(string subscriberId, Message message)
    {
        // This would be implemented by the broker to send message to the specific subscriber
        // For now, we'll use an event-based approach
        MessageDelivered?.Invoke(
            new MessageDeliveryEventArgs(subscriberId, message, _subscriptionId)
        );
        return true;
    }

    /// <summary>
    /// Event raised when a message needs to be delivered to a subscriber
    /// </summary>
    public event Action<MessageDeliveryEventArgs>? MessageDelivered;

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;
        _disposed = true;

        _cancellationTokenSource.Cancel();
        _messageWriter.Complete();

        try
        {
            await _deliveryTask;
        }
        catch (OperationCanceledException)
        {
            // Expected
        }

        await _reader.DisposeAsync();
        _cancellationTokenSource.Dispose();
    }
}

/// <summary>
/// Event arguments for message delivery
/// </summary>
public sealed record MessageDeliveryEventArgs(
    string SubscriberId,
    Message Message,
    string SubscriptionId
);

/// <summary>
/// Manager for all subscriptions with efficient lookup and management
/// </summary>
public sealed class SubscriptionManager : IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, Subscription> _subscriptions = new();
    private readonly ConcurrentDictionary<string, string> _subscriberToSubscription = new();
    private readonly Timer _cleanupTimer;
    private readonly TimeSpan _inactivityTimeout = TimeSpan.FromMinutes(5);
    private volatile bool _disposed;

    public SubscriptionManager()
    {
        // Setup periodic cleanup of inactive subscribers
        _cleanupTimer = new Timer(
            CleanupInactiveSubscribers,
            null,
            TimeSpan.FromMinutes(1),
            TimeSpan.FromMinutes(1)
        );
    }

    /// <summary>
    /// Create a new subscription
    /// </summary>
    public async Task<Subscription> CreateSubscriptionAsync(
        string subscriptionId,
        string topicName,
        SubscriptionReader reader
    )
    {
        if (_subscriptions.ContainsKey(subscriptionId))
        {
            throw new InvalidOperationException($"Subscription {subscriptionId} already exists");
        }

        var subscription = new Subscription(subscriptionId, topicName, reader);

        if (!_subscriptions.TryAdd(subscriptionId, subscription))
        {
            await subscription.DisposeAsync();
            throw new InvalidOperationException($"Failed to create subscription {subscriptionId}");
        }

        return subscription;
    }

    /// <summary>
    /// Get subscription by ID
    /// </summary>
    public Subscription? GetSubscription(string subscriptionId)
    {
        return _subscriptions.GetValueOrDefault(subscriptionId);
    }

    /// <summary>
    /// Delete a subscription
    /// </summary>
    public async Task<bool> DeleteSubscriptionAsync(string subscriptionId)
    {
        if (!_subscriptions.TryRemove(subscriptionId, out var subscription))
        {
            return false;
        }

        // Remove all subscribers from this subscription
        var subscribersToRemove = new List<string>();
        foreach (var (subscriberId, subId) in _subscriberToSubscription)
        {
            if (subId == subscriptionId)
            {
                subscribersToRemove.Add(subscriberId);
            }
        }

        foreach (var subscriberId in subscribersToRemove)
        {
            _subscriberToSubscription.TryRemove(subscriberId, out _);
        }

        await subscription.DisposeAsync();
        return true;
    }

    /// <summary>
    /// Add subscriber to a subscription
    /// </summary>
    public bool AddSubscriber(string subscriptionId, string subscriberId, string connectionId)
    {
        var subscription = GetSubscription(subscriptionId);
        if (subscription == null)
        {
            return false;
        }

        if (subscription.AddSubscriber(subscriberId, connectionId))
        {
            _subscriberToSubscription[subscriberId] = subscriptionId;
            return true;
        }

        return false;
    }

    /// <summary>
    /// Remove subscriber from their subscription
    /// </summary>
    public bool RemoveSubscriber(string subscriberId)
    {
        if (!_subscriberToSubscription.TryRemove(subscriberId, out var subscriptionId))
        {
            return false;
        }

        var subscription = GetSubscription(subscriptionId);
        return subscription?.RemoveSubscriber(subscriberId) ?? false;
    }

    /// <summary>
    /// Get subscription for a subscriber
    /// </summary>
    public Subscription? GetSubscriptionForSubscriber(string subscriberId)
    {
        if (_subscriberToSubscription.TryGetValue(subscriberId, out var subscriptionId))
        {
            return GetSubscription(subscriptionId);
        }
        return null;
    }

    /// <summary>
    /// List all subscriptions
    /// </summary>
    public IEnumerable<Subscription> GetAllSubscriptions()
    {
        return _subscriptions.Values.ToList();
    }

    /// <summary>
    /// Update subscriber activity
    /// </summary>
    public void UpdateSubscriberActivity(string subscriberId)
    {
        var subscription = GetSubscriptionForSubscriber(subscriberId);
        subscription?.UpdateSubscriberActivity(subscriberId);
    }

    /// <summary>
    /// Cleanup inactive subscribers
    /// </summary>
    private void CleanupInactiveSubscribers(object? state)
    {
        if (_disposed)
            return;

        var cutoffTime = DateTime.UtcNow - _inactivityTimeout;
        var inactiveSubscribers = new List<string>();

        foreach (var subscription in _subscriptions.Values)
        {
            foreach (var subscriber in subscription.GetSubscribers())
            {
                if (subscriber.LastActivity < cutoffTime)
                {
                    inactiveSubscribers.Add(subscriber.SubscriberId);
                }
            }
        }

        foreach (var subscriberId in inactiveSubscribers)
        {
            RemoveSubscriber(subscriberId);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;
        _disposed = true;

        _cleanupTimer?.Dispose();

        // Dispose all subscriptions
        await Task.WhenAll(_subscriptions.Values.Select(s => s.DisposeAsync().AsTask()));
        _subscriptions.Clear();
        _subscriberToSubscription.Clear();
    }
}
