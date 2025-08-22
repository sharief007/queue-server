namespace QueueServer.Core.Models;

/// <summary>
/// Message types for protocol operations
/// </summary>
public enum MessageType : uint
{
    Data = 1,
    Acknowledgment = 2,
    Heartbeat = 3,
    CreateTopic = 4,
    CreateSubscription = 5,
    DeleteSubscription = 6,
    Subscribe = 7,
    Unsubscribe = 8,
    Publish = 9
}

/// <summary>
/// Connection states for client connections
/// </summary>
public enum ConnectionState : byte
{
    Connecting = 1,
    Connected = 2,
    Disconnecting = 3,
    Disconnected = 4
}

/// <summary>
/// Subscription states
/// </summary>
public enum SubscriptionState : byte
{
    Active = 1,
    Paused = 2,
    Deleted = 3
}

/// <summary>
/// Storage operation results
/// </summary>
public enum StorageResult : byte
{
    Success = 1,
    TopicNotFound = 2,
    OffsetOutOfRange = 3,
    IoError = 4,
    InsufficientSpace = 5
}
