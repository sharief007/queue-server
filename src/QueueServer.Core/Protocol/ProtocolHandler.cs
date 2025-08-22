#pragma warning disable CS1998 // Async method lacks 'await' operators
using QueueServer.Core.Models;
using QueueServer.Core.Network;
using QueueServer.Core.Storage;
using QueueServer.Core.Subscriptions;

namespace QueueServer.Core.Protocol;

/// <summary>
/// Protocol request context
/// </summary>
public sealed record ProtocolRequest(
    ClientConnection Connection,
    Message Message,
    string RequestId
);

/// <summary>
/// Protocol response
/// </summary>
public sealed record ProtocolResponse(Message Message, bool Success, string? ErrorMessage = null);

/// <summary>
/// High-performance protocol handler for broker operations
/// </summary>
public sealed class ProtocolHandler
{
    private readonly SequentialStorageManager _storageManager;
    private readonly SubscriptionManager _subscriptionManager;

    public ProtocolHandler(
        SequentialStorageManager storageManager,
        SubscriptionManager subscriptionManager
    )
    {
        _storageManager = storageManager;
        _subscriptionManager = subscriptionManager;
    }

    /// <summary>
    /// Handle incoming protocol message
    /// </summary>
    public async Task<ProtocolResponse> HandleMessageAsync(ProtocolRequest request)
    {
        try
        {
            return request.Message.MessageType switch
            {
                MessageType.CreateTopic => await HandleCreateTopicAsync(request),
                MessageType.CreateSubscription => await HandleCreateSubscriptionAsync(request),
                MessageType.DeleteSubscription => await HandleDeleteSubscriptionAsync(request),
                MessageType.Subscribe => await HandleSubscribeAsync(request),
                MessageType.Unsubscribe => await HandleUnsubscribeAsync(request),
                MessageType.Publish => await HandlePublishAsync(request),
                MessageType.Heartbeat => HandleHeartbeat(request),
                _ => CreateErrorResponse(request.Message.SequenceNumber, "Unknown message type"),
            };
        }
        catch (Exception ex)
        {
            return CreateErrorResponse(
                request.Message.SequenceNumber,
                $"Error processing request: {ex.Message}"
            );
        }
    }

    /// <summary>
    /// Handle create topic request
    /// </summary>
    private Task<ProtocolResponse> HandleCreateTopicAsync(ProtocolRequest request)
    {
        var topicName = ExtractProperty(request.Message, "topic");
        if (string.IsNullOrEmpty(topicName))
        {
            return Task.FromResult(CreateErrorResponse(request.Message.SequenceNumber, "Topic name is required"));
        }

        try
        {
            var topicStorage = _storageManager.GetOrCreateTopic(topicName);

            var response = new MessageBuilder(MessageType.Acknowledgment)
                .WithSequenceNumber(request.Message.SequenceNumber)
                .WithProperty("status", "success")
                .WithProperty("topic", topicName)
                .Build();

            return Task.FromResult(new ProtocolResponse(response, true));
        }
        catch (Exception ex)
        {
            return Task.FromResult(CreateErrorResponse(request.Message.SequenceNumber, $"Failed to create topic: {ex.Message}"));
        }
    }

    /// <summary>
    /// Handle create subscription request
    /// </summary>
    private async Task<ProtocolResponse> HandleCreateSubscriptionAsync(ProtocolRequest request)
    {
        var subscriptionId = ExtractProperty(request.Message, "subscription_id");
        var topicName = ExtractProperty(request.Message, "topic");
        var startOffsetStr = ExtractProperty(request.Message, "start_offset") ?? "0";

        if (string.IsNullOrEmpty(subscriptionId) || string.IsNullOrEmpty(topicName))
        {
            return CreateErrorResponse(
                request.Message.SequenceNumber,
                "Subscription ID and topic name are required"
            );
        }

        if (!ulong.TryParse(startOffsetStr, out var startOffset))
        {
            return CreateErrorResponse(request.Message.SequenceNumber, "Invalid start offset");
        }

        try
        {
            var topicStorage = _storageManager.GetOrCreateTopic(topicName);
            var reader = topicStorage.CreateSubscriptionReader(subscriptionId, startOffset);
            var subscription = await _subscriptionManager.CreateSubscriptionAsync(
                subscriptionId,
                topicName,
                reader
            );

            var response = new MessageBuilder(MessageType.Acknowledgment)
                .WithSequenceNumber(request.Message.SequenceNumber)
                .WithProperty("status", "success")
                .WithProperty("subscription_id", subscriptionId)
                .WithProperty("topic", topicName)
                .WithProperty("start_offset", startOffset.ToString())
                .Build();

            return new ProtocolResponse(response, true);
        }
        catch (Exception ex)
        {
            return CreateErrorResponse(
                request.Message.SequenceNumber,
                $"Failed to create subscription: {ex.Message}"
            );
        }
    }

    /// <summary>
    /// Handle delete subscription request
    /// </summary>
    private async Task<ProtocolResponse> HandleDeleteSubscriptionAsync(ProtocolRequest request)
    {
        var subscriptionId = ExtractProperty(request.Message, "subscription_id");
        if (string.IsNullOrEmpty(subscriptionId))
        {
            return CreateErrorResponse(
                request.Message.SequenceNumber,
                "Subscription ID is required"
            );
        }

        try
        {
            var deleted = await _subscriptionManager.DeleteSubscriptionAsync(subscriptionId);
            var status = deleted ? "success" : "not_found";

            var response = new MessageBuilder(MessageType.Acknowledgment)
                .WithSequenceNumber(request.Message.SequenceNumber)
                .WithProperty("status", status)
                .WithProperty("subscription_id", subscriptionId)
                .Build();

            return new ProtocolResponse(response, true);
        }
        catch (Exception ex)
        {
            return CreateErrorResponse(
                request.Message.SequenceNumber,
                $"Failed to delete subscription: {ex.Message}"
            );
        }
    }

    /// <summary>
    /// Handle subscribe request
    /// </summary>
    private async Task<ProtocolResponse> HandleSubscribeAsync(ProtocolRequest request)
    {
        var subscriptionId = ExtractProperty(request.Message, "subscription_id");
        if (string.IsNullOrEmpty(subscriptionId))
        {
            return CreateErrorResponse(
                request.Message.SequenceNumber,
                "Subscription ID is required"
            );
        }

        try
        {
            var subscriberId = request.Connection.ConnectionId;
            var added = _subscriptionManager.AddSubscriber(
                subscriptionId,
                subscriberId,
                request.Connection.ConnectionId
            );

            if (!added)
            {
                return CreateErrorResponse(
                    request.Message.SequenceNumber,
                    "Subscription not found or subscriber already exists"
                );
            }

            var subscription = _subscriptionManager.GetSubscription(subscriptionId);
            var response = new MessageBuilder(MessageType.Acknowledgment)
                .WithSequenceNumber(request.Message.SequenceNumber)
                .WithProperty("status", "success")
                .WithProperty("subscription_id", subscriptionId)
                .WithProperty("topic", subscription?.TopicName ?? "unknown")
                .Build();

            return new ProtocolResponse(response, true);
        }
        catch (Exception ex)
        {
            return CreateErrorResponse(
                request.Message.SequenceNumber,
                $"Failed to subscribe: {ex.Message}"
            );
        }
    }

    /// <summary>
    /// Handle unsubscribe request
    /// </summary>
    private async Task<ProtocolResponse> HandleUnsubscribeAsync(ProtocolRequest request)
    {
        var subscriptionId = ExtractProperty(request.Message, "subscription_id");
        if (string.IsNullOrEmpty(subscriptionId))
        {
            return CreateErrorResponse(
                request.Message.SequenceNumber,
                "Subscription ID is required"
            );
        }

        try
        {
            var subscriberId = request.Connection.ConnectionId;
            var removed = _subscriptionManager.RemoveSubscriber(subscriberId);
            var status = removed ? "success" : "not_found";

            var response = new MessageBuilder(MessageType.Acknowledgment)
                .WithSequenceNumber(request.Message.SequenceNumber)
                .WithProperty("status", status)
                .WithProperty("subscription_id", subscriptionId)
                .Build();

            return new ProtocolResponse(response, true);
        }
        catch (Exception ex)
        {
            return CreateErrorResponse(
                request.Message.SequenceNumber,
                $"Failed to unsubscribe: {ex.Message}"
            );
        }
    }

    /// <summary>
    /// Handle publish request
    /// </summary>
    private async Task<ProtocolResponse> HandlePublishAsync(ProtocolRequest request)
    {
        var topicName = ExtractProperty(request.Message, "topic");
        if (string.IsNullOrEmpty(topicName))
        {
            return CreateErrorResponse(request.Message.SequenceNumber, "Topic name is required");
        }

        try
        {
            var topicStorage = _storageManager.GetOrCreateTopic(topicName);

            // Create data message with the published content
            var dataMessage = new MessageBuilder(MessageType.Data)
                .WithSequenceNumber(0) // Will be set by storage
                .WithProperty("topic", topicName)
                .WithProperty("publisher_id", request.Connection.ConnectionId)
                .WithBody(request.Message.Body.Span)
                .Build();

            // Copy additional properties from original message
            CopyProperties(request.Message, dataMessage);

            // Store message
            var offset = await topicStorage.AppendMessageAsync(dataMessage);

            // Update sequence number with actual offset
            var storedMessage = new MessageBuilder(MessageType.Data)
                .WithSequenceNumber(offset)
                .WithProperty("topic", topicName)
                .WithProperty("publisher_id", request.Connection.ConnectionId)
                .WithBody(request.Message.Body.Span)
                .Build();

            CopyProperties(request.Message, storedMessage);

            // Notify all subscriptions for this topic
            await NotifySubscriptionsAsync(topicName, storedMessage);

            // Send acknowledgment
            var response = new MessageBuilder(MessageType.Acknowledgment)
                .WithSequenceNumber(request.Message.SequenceNumber)
                .WithProperty("status", "success")
                .WithProperty("topic", topicName)
                .WithProperty("offset", offset.ToString())
                .Build();

            return new ProtocolResponse(response, true);
        }
        catch (Exception ex)
        {
            return CreateErrorResponse(
                request.Message.SequenceNumber,
                $"Failed to publish message: {ex.Message}"
            );
        }
    }

    /// <summary>
    /// Handle heartbeat request
    /// </summary>
    private ProtocolResponse HandleHeartbeat(ProtocolRequest request)
    {
        request.Connection.UpdateHeartbeat();

        var response = new MessageBuilder(MessageType.Heartbeat)
            .WithSequenceNumber(request.Message.SequenceNumber)
            .WithProperty("status", "ok")
            .WithProperty("server_time", DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString())
            .Build();

        return new ProtocolResponse(response, true);
    }

    /// <summary>
    /// Notify all subscriptions for a topic about a new message
    /// </summary>
    private async Task NotifySubscriptionsAsync(string topicName, Message message)
    {
        var subscriptions = _subscriptionManager
            .GetAllSubscriptions()
            .Where(s => s.TopicName == topicName)
            .ToList();

        foreach (var subscription in subscriptions)
        {
            try
            {
                await subscription.EnqueueMessageAsync(message);
            }
            catch (Exception ex)
            {
                Console.WriteLine(
                    $"Error notifying subscription {subscription.SubscriptionId}: {ex.Message}"
                );
            }
        }
    }

    /// <summary>
    /// Extract property from message
    /// </summary>
    private static string? ExtractProperty(Message message, string key)
    {
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
            // Find key=value\0 pattern
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

    /// <summary>
    /// Copy properties from source message to target message
    /// </summary>
    private static void CopyProperties(Message source, Message target)
    {
        // This would require modifying the message structure to support property copying
        // For now, properties are immutable in the Message struct
    }

    /// <summary>
    /// Create error response
    /// </summary>
    private static ProtocolResponse CreateErrorResponse(ulong sequenceNumber, string errorMessage)
    {
        var response = new MessageBuilder(MessageType.Acknowledgment)
            .WithSequenceNumber(sequenceNumber)
            .WithProperty("status", "error")
            .WithProperty("error", errorMessage)
            .Build();

        return new ProtocolResponse(response, false, errorMessage);
    }
}
