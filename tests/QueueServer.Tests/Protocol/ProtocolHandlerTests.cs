using System.Text;
using FluentAssertions;
using QueueServer.Core.Configuration;
using QueueServer.Core.Models;
using QueueServer.Core.Network;
using QueueServer.Core.Protocol;
using QueueServer.Core.Storage;
using QueueServer.Core.Subscriptions;
using Xunit;

namespace QueueServer.Tests.Protocol;

/// <summary>
/// Comprehensive unit tests for ProtocolHandler
/// Tests cover message processing, error handling, and protocol compliance
/// </summary>
public class ProtocolHandlerTests : IAsyncDisposable
{
    private readonly StorageConfiguration _storageConfig;
    private readonly string _testDataPath;
    private readonly List<SequentialStorageManager> _storageManagers = new();
    private readonly List<SubscriptionManager> _subscriptionManagers = new();
    private readonly List<ProtocolHandler> _protocolHandlers = new();

    public ProtocolHandlerTests()
    {
        _testDataPath = Path.Combine(Path.GetTempPath(), $"protocol_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDataPath);

        _storageConfig = new StorageConfiguration
        {
            DataDirectory = _testDataPath,
            FsyncInterval = TimeSpan.FromMilliseconds(100),
        };
    }

    [Fact]
    public void Constructor_WithValidParameters_ShouldInitializeCorrectly()
    {
        // Arrange & Act
        var handler = CreateProtocolHandler();

        // Assert
        handler.Should().NotBeNull();
    }

    [Fact]
    public async Task HandleMessageAsync_CreateTopicRequest_ShouldCreateTopic()
    {
        // Arrange
        var handler = CreateProtocolHandler();
        var createTopicMessage = CreateMessage(MessageType.CreateTopic, "topic=test_topic", "");
        var request = CreateProtocolRequest(createTopicMessage);

        // Act
        var response = await handler.HandleMessageAsync(request);

        // Assert
        response.Should().NotBeNull();
        response.Success.Should().BeTrue();
        response.Message.MessageType.Should().Be(MessageType.Acknowledgment);
    }

    [Fact]
    public async Task HandleMessageAsync_SubscribeRequest_ShouldCreateSubscription()
    {
        // Arrange
        var handler = CreateProtocolHandler();
        var subscriptionId = "sub123";

        // First create a subscription
        var createProperties = $"subscription_id={subscriptionId};topic=test_topic;start_offset=0";
        var createMessage = CreateMessage(MessageType.CreateSubscription, createProperties, "");
        var createRequest = CreateProtocolRequest(createMessage);
        await handler.HandleMessageAsync(createRequest);

        // Then subscribe to it
        var subscribeProperties = $"subscription_id={subscriptionId}";
        var subscribeMessage = CreateMessage(MessageType.Subscribe, subscribeProperties, "");
        var subscribeRequest = CreateProtocolRequest(subscribeMessage);

        // Act
        var response = await handler.HandleMessageAsync(subscribeRequest);

        // Assert
        response.Should().NotBeNull();
        response.Success.Should().BeTrue();
        response.Message.MessageType.Should().Be(MessageType.Acknowledgment);
    }

    [Fact]
    public async Task HandleMessageAsync_UnsubscribeRequest_ShouldRemoveSubscription()
    {
        // Arrange
        var handler = CreateProtocolHandler();
        var properties = "subscription_id=sub123"; // Fixed: was subscriber_id, should be subscription_id

        // First create a subscription
        var subscribeMessage = CreateMessage(MessageType.Subscribe, properties, "");
        var subscribeRequest = CreateProtocolRequest(subscribeMessage);
        await handler.HandleMessageAsync(subscribeRequest);

        // Then unsubscribe
        var unsubscribeMessage = CreateMessage(MessageType.Unsubscribe, properties, "");
        var unsubscribeRequest = CreateProtocolRequest(unsubscribeMessage);

        // Act
        var response = await handler.HandleMessageAsync(unsubscribeRequest);

        // Assert
        response.Should().NotBeNull();
        response.Success.Should().BeTrue();
        response.Message.MessageType.Should().Be(MessageType.Acknowledgment);
    }

    [Fact]
    public async Task HandleMessageAsync_PublishRequest_ShouldStoreMessage()
    {
        // Arrange
        var handler = CreateProtocolHandler();
        var properties = "topic=test_topic";
        var messageBody = "Hello, World!";
        var publishMessage = CreateMessage(MessageType.Publish, properties, messageBody);
        var request = CreateProtocolRequest(publishMessage);

        // Act
        var response = await handler.HandleMessageAsync(request);

        // Assert
        response.Should().NotBeNull();
        response.Success.Should().BeTrue();
        response.Message.MessageType.Should().Be(MessageType.Acknowledgment);
    }

    [Fact]
    public async Task HandleMessageAsync_HeartbeatRequest_ShouldReturnAcknowledgment()
    {
        // Arrange
        var handler = CreateProtocolHandler();
        var heartbeatMessage = CreateMessage(MessageType.Heartbeat, "", "");
        var request = CreateProtocolRequest(heartbeatMessage);

        // Act
        var response = await handler.HandleMessageAsync(request);

        // Assert
        response.Should().NotBeNull();
        response.Success.Should().BeTrue();
        response.Message.MessageType.Should().Be(MessageType.Heartbeat); // Heartbeat should return Heartbeat, not Acknowledgment
    }

    [Theory]
    [InlineData(MessageType.Data)]
    public async Task HandleMessageAsync_UnsupportedMessageTypes_ShouldReturnError(
        MessageType messageType
    )
    {
        // Arrange
        var handler = CreateProtocolHandler();
        var message = CreateMessage(messageType, "", "");
        var request = CreateProtocolRequest(message);

        // Act
        var response = await handler.HandleMessageAsync(request);

        // Assert
        response.Should().NotBeNull();
        response.Success.Should().BeFalse();
        response.ErrorMessage.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task HandleMessageAsync_LargeMessage_ShouldProcessCorrectly()
    {
        // Arrange
        var handler = CreateProtocolHandler();
        var properties = "topic=large_messages";
        var largeBody = new string('A', 50_000); // 50KB message
        var publishMessage = CreateMessage(MessageType.Publish, properties, largeBody);
        var request = CreateProtocolRequest(publishMessage);

        // Act
        var response = await handler.HandleMessageAsync(request);

        // Assert
        response.Should().NotBeNull();
        response.Success.Should().BeTrue();
        response.Message.MessageType.Should().Be(MessageType.Acknowledgment);
    }

    [Fact]
    public async Task HandleMessageAsync_UnicodeContent_ShouldProcessCorrectly()
    {
        // Arrange
        var handler = CreateProtocolHandler();
        var properties = "topic=unicode_test";
        var unicodeBody = "こんにちは世界! 🌍 Hello World! 🎉";
        var publishMessage = CreateMessage(MessageType.Publish, properties, unicodeBody);
        var request = CreateProtocolRequest(publishMessage);

        // Act
        var response = await handler.HandleMessageAsync(request);

        // Assert
        response.Should().NotBeNull();
        response.Success.Should().BeTrue();
        response.Message.MessageType.Should().Be(MessageType.Acknowledgment);
    }

    [Fact]
    public async Task HandleMessageAsync_ConcurrentRequests_ShouldProcessAllCorrectly()
    {
        // Arrange
        var handler = CreateProtocolHandler();
        const int requestCount = 100;
        var tasks = new List<Task<ProtocolResponse>>();

        // Act - Send concurrent publish requests
        for (int i = 0; i < requestCount; i++)
        {
            var properties = $"topic=concurrent_test";
            var body = $"Message {i}";
            var message = CreateMessage(MessageType.Publish, properties, body);
            var request = CreateProtocolRequest(message);

            var task = handler.HandleMessageAsync(request);
            tasks.Add(task);
        }

        var responses = await Task.WhenAll(tasks);

        // Assert
        responses.Should().HaveCount(requestCount);
        responses
            .Should()
            .AllSatisfy(response =>
            {
                response.Should().NotBeNull();
                response.Success.Should().BeTrue();
                response.Message.MessageType.Should().Be(MessageType.Acknowledgment);
            });
    }

    [Fact]
    public async Task HandleMessageAsync_Performance_ShouldMeetBenchmarks()
    {
        // Arrange
        var handler = CreateProtocolHandler();
        const int messageCount = 1000;
        var requests = new List<ProtocolRequest>();

        for (int i = 0; i < messageCount; i++)
        {
            var properties = $"topic=perf_test";
            var body = $"Performance test message {i}";
            var message = CreateMessage(MessageType.Publish, properties, body);
            requests.Add(CreateProtocolRequest(message));
        }

        // Act - Time the operations
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        foreach (var request in requests)
        {
            await handler.HandleMessageAsync(request);
        }

        stopwatch.Stop();

        // Assert
        stopwatch
            .ElapsedMilliseconds.Should()
            .BeLessThan(10000, "Processing 1000 messages should complete in under 10 seconds");
    }

    [Fact]
    public async Task HandleMessageAsync_InvalidTopicName_ShouldReturnError()
    {
        // Arrange
        var handler = CreateProtocolHandler();
        var messageWithoutTopic = CreateMessage(MessageType.CreateTopic, "", "");
        var request = CreateProtocolRequest(messageWithoutTopic);

        // Act
        var response = await handler.HandleMessageAsync(request);

        // Assert
        response.Should().NotBeNull();
        response.Success.Should().BeFalse();
        response.ErrorMessage.Should().Contain("Topic name is required");
    }

    [Fact]
    public async Task HandleMessageAsync_MalformedProperties_ShouldHandleGracefully()
    {
        // Arrange
        var handler = CreateProtocolHandler();

        // Create a message with malformed properties
        var malformedMessage = new Message(
            MessageType.Subscribe,
            1UL,
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            Encoding.UTF8.GetBytes("invalid_properties_format_no_equals"),
            Array.Empty<byte>()
        );
        var request = CreateProtocolRequest(malformedMessage);

        // Act
        var response = await handler.HandleMessageAsync(request);

        // Assert
        response.Should().NotBeNull();
        // The handler should handle this gracefully, either with success or a meaningful error
    }

    [Fact]
    public async Task HandleMessageAsync_EmptyMessages_ShouldHandleGracefully()
    {
        // Arrange
        var handler = CreateProtocolHandler();
        var emptyMessage = CreateMessage(MessageType.Heartbeat, "", "");
        var request = CreateProtocolRequest(emptyMessage);

        // Act
        var response = await handler.HandleMessageAsync(request);

        // Assert
        response.Should().NotBeNull();
        response.Success.Should().BeTrue();
        response.Message.MessageType.Should().Be(MessageType.Heartbeat); // Empty heartbeat should still return heartbeat
    }

    [Fact]
    public async Task HandleMessageAsync_MessageSequencing_ShouldPreserveSequenceNumbers()
    {
        // Arrange
        var handler = CreateProtocolHandler();
        var responses = new List<ProtocolResponse>();

        // Act - Send messages with sequence numbers
        for (ulong i = 1; i <= 10; i++)
        {
            // Use the proper message creation helper that formats properties correctly
            var message = CreateMessage(MessageType.Publish, "topic=sequence_test", $"Message {i}");
            // Override the sequence number to test sequencing
            var customMessage = new Message(
                message.MessageType,
                i,
                message.Timestamp,
                message.Properties,
                message.Body
            );
            var request = CreateProtocolRequest(customMessage);

            var response = await handler.HandleMessageAsync(request);
            responses.Add(response);
        }

        // Assert
        responses.Should().HaveCount(10);
        for (int i = 0; i < responses.Count; i++)
        {
            responses[i].Success.Should().BeTrue();
            responses[i].Message.SequenceNumber.Should().Be((ulong)(i + 1));
        }
    }

    [Fact]
    public async Task HandleMessageAsync_CreateSubscriptionRequest_ShouldProcessCorrectly()
    {
        // Arrange
        var handler = CreateProtocolHandler();
        var properties = "subscription_id=client1;topic=notifications;start_offset=0"; // Fixed: needs subscription_id and topic
        var message = CreateMessage(MessageType.CreateSubscription, properties, "");
        var request = CreateProtocolRequest(message);

        // Act
        var response = await handler.HandleMessageAsync(request);

        // Assert
        response.Should().NotBeNull();
        response.Success.Should().BeTrue();
        response.Message.MessageType.Should().Be(MessageType.Acknowledgment);
    }

    [Fact]
    public async Task HandleMessageAsync_DeleteSubscriptionRequest_ShouldProcessCorrectly()
    {
        // Arrange
        var handler = CreateProtocolHandler();
        var createProperties = "subscription_id=client1;topic=notifications;start_offset=0"; // For creating
        var deleteProperties = "subscription_id=client1"; // For deleting

        // First create a subscription
        var createMessage = CreateMessage(MessageType.CreateSubscription, createProperties, "");
        var createRequest = CreateProtocolRequest(createMessage);
        await handler.HandleMessageAsync(createRequest);

        // Then delete it
        var deleteMessage = CreateMessage(MessageType.DeleteSubscription, deleteProperties, "");
        var deleteRequest = CreateProtocolRequest(deleteMessage);

        // Act
        var response = await handler.HandleMessageAsync(deleteRequest);

        // Assert
        response.Should().NotBeNull();
        response.Success.Should().BeTrue();
        response.Message.MessageType.Should().Be(MessageType.Acknowledgment);
    }

    private ProtocolHandler CreateProtocolHandler()
    {
        var storageManager = new SequentialStorageManager(_storageConfig);
        var subscriptionManager = new SubscriptionManager();
        var handler = new ProtocolHandler(storageManager, subscriptionManager);

        _storageManagers.Add(storageManager);
        _subscriptionManagers.Add(subscriptionManager);
        _protocolHandlers.Add(handler);

        return handler;
    }

    private ProtocolRequest CreateProtocolRequest(Message message)
    {
        // Create a minimal test connection that provides the required ConnectionId
        // For testing, we use reflection to create a connection instance since
        // ClientConnection is sealed and has complex dependencies
        var testConnectionId = $"test_connection_{Guid.NewGuid():N}";

        // Since we can't easily mock or create ClientConnection, we'll create a request
        // without connection for tests that don't need it, and use a different approach
        // for tests that do need it
        return new ProtocolRequest(
            CreateTestConnection(testConnectionId),
            message,
            $"req_{Guid.NewGuid():N}"
        );
    }

    private static ClientConnection CreateTestConnection(string connectionId)
    {
        // Use reflection to create a test connection since ClientConnection
        // constructor requires Socket and ServerConfiguration
        // This is a test-only approach to provide the minimal interface needed
        try
        {
            // Create a dummy socket just for testing
            var socket = new System.Net.Sockets.Socket(
                System.Net.Sockets.AddressFamily.InterNetwork,
                System.Net.Sockets.SocketType.Stream,
                System.Net.Sockets.ProtocolType.Tcp
            );

            var config = new QueueServer.Core.Configuration.ServerConfiguration();
            return new ClientConnection(socket, connectionId, config);
        }
        catch
        {
            // If that fails, fall back to null and update the tests
            return null!;
        }
    }

    private static Message CreateMessage(MessageType messageType, string properties, string body)
    {
        // Format properties correctly with null terminators as expected by ParseProperties
        var propertiesBytes = FormatProperties(properties);
        var bodyBytes = Encoding.UTF8.GetBytes(body);

        return new Message(
            messageType,
            (ulong)Random.Shared.NextInt64(1, long.MaxValue),
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            propertiesBytes,
            bodyBytes
        );
    }

    private static byte[] FormatProperties(string properties)
    {
        if (string.IsNullOrEmpty(properties))
            return Array.Empty<byte>();

        // Handle multiple properties separated by semicolons
        var keyValuePairs = properties.Split(';', StringSplitOptions.RemoveEmptyEntries);
        var formattedProperties = new List<byte>();

        foreach (var kvp in keyValuePairs)
        {
            if (kvp.Contains('='))
            {
                // Add the key=value string
                var kvpBytes = Encoding.UTF8.GetBytes(kvp);
                formattedProperties.AddRange(kvpBytes);
                // Add null terminator
                formattedProperties.Add(0);
            }
        }

        return formattedProperties.ToArray();
    }

    public ValueTask DisposeAsync()
    {
        foreach (var handler in _protocolHandlers)
        {
            // ProtocolHandler doesn't implement IDisposable in this implementation
        }

        foreach (var subscriptionManager in _subscriptionManagers)
        {
            // SubscriptionManager doesn't implement IDisposable in this implementation
        }

        foreach (var storageManager in _storageManagers)
        {
            storageManager.Dispose();
        }

        // Cleanup test directory
        if (Directory.Exists(_testDataPath))
        {
            try
            {
                Directory.Delete(_testDataPath, true);
            }
            catch
            {
                // Ignore cleanup errors in tests
            }
        }

        return ValueTask.CompletedTask;
    }
}
