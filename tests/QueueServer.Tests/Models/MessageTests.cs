using System.Text;
using FluentAssertions;
using QueueServer.Core.Models;
using Xunit;

namespace QueueServer.Tests.Models;

/// <summary>
/// Comprehensive unit tests for the Message struct
/// Tests cover serialization, deserialization, performance, and edge cases
/// </summary>
public class MessageTests : IDisposable
{
    private readonly List<Message> _messagesToDispose = new();

    [Fact]
    public void Constructor_WithBasicParameters_ShouldCreateValidMessage()
    {
        // Arrange
        var messageType = MessageType.Publish;
        var sequenceNumber = 12345UL;
        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var properties = "key1=value1;key2=value2"u8.ToArray();
        var body = "Hello, World!"u8.ToArray();

        // Act
        var message = new Message(messageType, sequenceNumber, timestamp, properties, body);
        _messagesToDispose.Add(message);

        // Assert
        message.MessageType.Should().Be(messageType);
        message.SequenceNumber.Should().Be(sequenceNumber);
        message.Timestamp.Should().Be(timestamp);
        message.Properties.ToArray().Should().BeEquivalentTo(properties);
        message.Body.ToArray().Should().BeEquivalentTo(body);
    }

    [Fact]
    public void Constructor_WithEmptyProperties_ShouldCreateValidMessage()
    {
        // Arrange
        var messageType = MessageType.Subscribe;
        var body = "Test message"u8.ToArray();

        // Act
        var message = new Message(
            messageType,
            1UL,
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            Array.Empty<byte>(),
            body
        );
        _messagesToDispose.Add(message);

        // Assert
        message.Properties.IsEmpty.Should().BeTrue();
        message.Body.ToArray().Should().BeEquivalentTo(body);
    }

    [Fact]
    public void Constructor_WithEmptyBody_ShouldCreateValidMessage()
    {
        // Arrange
        var messageType = MessageType.Unsubscribe;
        var properties = "topic=events"u8.ToArray();

        // Act
        var message = new Message(
            messageType,
            1UL,
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            properties,
            Array.Empty<byte>()
        );
        _messagesToDispose.Add(message);

        // Assert
        message.Properties.ToArray().Should().BeEquivalentTo(properties);
        message.Body.IsEmpty.Should().BeTrue();
    }

    [Fact]
    public void Serialize_ValidMessage_ShouldReturnCorrectBinaryFormat()
    {
        // Arrange
        var message = new Message(
            MessageType.Publish,
            100UL,
            1234567890L,
            "prop=test"u8.ToArray(),
            "body data"u8.ToArray()
        );
        _messagesToDispose.Add(message);

        var buffer = new byte[message.TotalSize];

        // Act
        message.Serialize(buffer);

        // Assert
        buffer.Length.Should().BeGreaterThan(Message.HeaderSize);

        // Verify header structure
        var totalLength = BitConverter.ToInt32(buffer, 0);
        var messageType = (MessageType)BitConverter.ToUInt32(buffer, 4);
        var sequenceNumber = BitConverter.ToUInt64(buffer, 8);
        var timestamp = BitConverter.ToInt64(buffer, 16);

        totalLength.Should().Be(buffer.Length);
        messageType.Should().Be(MessageType.Publish);
        sequenceNumber.Should().Be(100UL);
        timestamp.Should().Be(1234567890L);
    }

    [Fact]
    public void Deserialize_ValidBinaryData_ShouldReturnCorrectMessage()
    {
        // Arrange
        var originalMessage = new Message(
            MessageType.Subscribe,
            500UL,
            9876543210L,
            "subscription=logs"u8.ToArray(),
            "filter criteria"u8.ToArray()
        );
        _messagesToDispose.Add(originalMessage);

        var buffer = new byte[originalMessage.TotalSize];
        originalMessage.Serialize(buffer);

        // Act
        var (deserializedMessage, bytesRead) = Message.Deserialize(buffer);
        _messagesToDispose.Add(deserializedMessage);

        // Assert
        deserializedMessage.MessageType.Should().Be(originalMessage.MessageType);
        deserializedMessage.SequenceNumber.Should().Be(originalMessage.SequenceNumber);
        deserializedMessage.Timestamp.Should().Be(originalMessage.Timestamp);
        deserializedMessage
            .Properties.ToArray()
            .Should()
            .BeEquivalentTo(originalMessage.Properties.ToArray());
        deserializedMessage.Body.ToArray().Should().BeEquivalentTo(originalMessage.Body.ToArray());
        bytesRead.Should().Be(originalMessage.TotalSize);
    }

    [Fact]
    public void Serialize_Deserialize_RoundTrip_ShouldPreserveAllData()
    {
        // Arrange
        var properties = Encoding.UTF8.GetBytes("topic=events;priority=high;retry=3");
        var body = Encoding.UTF8.GetBytes(
            "{'event': 'user_login', 'userId': 12345, 'timestamp': '2024-01-01T00:00:00Z'}"
        );
        var original = new Message(
            MessageType.Publish,
            9999UL,
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            properties,
            body
        );
        _messagesToDispose.Add(original);

        var buffer = new byte[original.TotalSize];

        // Act
        original.Serialize(buffer);
        var (deserialized, _) = Message.Deserialize(buffer);
        _messagesToDispose.Add(deserialized);

        // Assert
        deserialized.MessageType.Should().Be(original.MessageType);
        deserialized.SequenceNumber.Should().Be(original.SequenceNumber);
        deserialized.Timestamp.Should().Be(original.Timestamp);
        deserialized.Properties.ToArray().Should().BeEquivalentTo(original.Properties.ToArray());
        deserialized.Body.ToArray().Should().BeEquivalentTo(original.Body.ToArray());
    }

    [Fact]
    public void TotalSize_ValidMessage_ShouldReturnCorrectSize()
    {
        // Arrange
        var properties = "test=data"u8.ToArray();
        var body = "message body"u8.ToArray();
        var message = new Message(MessageType.Publish, 1UL, 0L, properties, body);
        _messagesToDispose.Add(message);

        // Act
        var totalSize = message.TotalSize;

        // Assert
        var expectedSize = Message.HeaderSize + properties.Length + body.Length;
        totalSize.Should().Be(expectedSize);
    }

    [Theory]
    [InlineData(MessageType.Publish)]
    [InlineData(MessageType.Subscribe)]
    [InlineData(MessageType.Unsubscribe)]
    [InlineData(MessageType.Data)]
    [InlineData(MessageType.Acknowledgment)]
    [InlineData(MessageType.Heartbeat)]
    [InlineData(MessageType.CreateTopic)]
    public void Constructor_WithDifferentMessageTypes_ShouldCreateValidMessages(
        MessageType messageType
    )
    {
        // Arrange & Act
        var message = new Message(
            messageType,
            1UL,
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            Array.Empty<byte>(),
            Array.Empty<byte>()
        );
        _messagesToDispose.Add(message);

        // Assert
        message.MessageType.Should().Be(messageType);
    }

    [Fact]
    public void Deserialize_InvalidData_ShouldThrowException()
    {
        // Arrange
        var invalidData = new byte[10]; // Too small to contain a valid header

        // Act & Assert
        Assert.Throws<ArgumentException>(() => Message.Deserialize(invalidData));
    }

    [Fact]
    public void Serialize_BufferTooSmall_ShouldThrowException()
    {
        // Arrange
        var message = new Message(
            MessageType.Publish,
            1UL,
            0L,
            "data"u8.ToArray(),
            "body"u8.ToArray()
        );
        _messagesToDispose.Add(message);
        var smallBuffer = new byte[10]; // Too small

        // Act & Assert
        Assert.Throws<ArgumentException>(() => message.Serialize(smallBuffer));
    }

    [Fact]
    public void Performance_SerializeDeserialize_ShouldBeEfficient()
    {
        // Arrange
        var properties = Encoding.UTF8.GetBytes("topic=performance_test;batch=1000");
        var body = Encoding.UTF8.GetBytes(new string('A', 1000)); // 1KB body
        var message = new Message(
            MessageType.Publish,
            1UL,
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            properties,
            body
        );
        _messagesToDispose.Add(message);

        const int iterations = 1000;
        var messages = new List<Message>();
        var buffer = new byte[message.TotalSize];

        // Act & Time the operation
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        for (int i = 0; i < iterations; i++)
        {
            message.Serialize(buffer);
            var (deserialized, _) = Message.Deserialize(buffer);
            messages.Add(deserialized);
        }

        stopwatch.Stop();

        // Cleanup
        foreach (var msg in messages)
        {
            msg.Dispose();
        }

        // Assert
        stopwatch
            .ElapsedMilliseconds.Should()
            .BeLessThan(
                1000,
                "1000 serialize/deserialize operations should complete in under 1 second"
            );
    }

    [Fact]
    public void LargeMessage_SerializeDeserialize_ShouldHandleCorrectly()
    {
        // Arrange
        var largeProperties = Encoding.UTF8.GetBytes(new string('P', 10000)); // 10KB properties
        var largeBody = Encoding.UTF8.GetBytes(new string('B', 100000)); // 100KB body
        var message = new Message(
            MessageType.Publish,
            ulong.MaxValue,
            long.MaxValue,
            largeProperties,
            largeBody
        );
        _messagesToDispose.Add(message);

        var buffer = new byte[message.TotalSize];

        // Act
        message.Serialize(buffer);
        var (deserialized, _) = Message.Deserialize(buffer);
        _messagesToDispose.Add(deserialized);

        // Assert
        deserialized.Properties.ToArray().Should().BeEquivalentTo(largeProperties);
        deserialized.Body.ToArray().Should().BeEquivalentTo(largeBody);
        deserialized.SequenceNumber.Should().Be(ulong.MaxValue);
        deserialized.Timestamp.Should().Be(long.MaxValue);
    }

    [Fact]
    public void Dispose_ShouldReleaseResources()
    {
        // Arrange
        var message = new Message(
            MessageType.Publish,
            1UL,
            0L,
            "test"u8.ToArray(),
            "data"u8.ToArray()
        );

        // Act
        message.Dispose();

        // Assert - No exception should be thrown
        // Note: We can't directly test memory release, but we ensure no exceptions
        Assert.True(true, "Message disposed without exceptions");
    }

    [Fact]
    public void MessageType_Values_ShouldMatchExpectedEnumValues()
    {
        // Arrange & Act & Assert
        ((uint)MessageType.Data)
            .Should()
            .Be(1);
        ((uint)MessageType.Acknowledgment).Should().Be(2);
        ((uint)MessageType.Heartbeat).Should().Be(3);
        ((uint)MessageType.CreateTopic).Should().Be(4);
        ((uint)MessageType.CreateSubscription).Should().Be(5);
        ((uint)MessageType.DeleteSubscription).Should().Be(6);
        ((uint)MessageType.Subscribe).Should().Be(7);
        ((uint)MessageType.Unsubscribe).Should().Be(8);
        ((uint)MessageType.Publish).Should().Be(9);
    }

    [Fact]
    public void Message_WithUnicodeContent_ShouldHandleCorrectly()
    {
        // Arrange
        var unicodeProperties = Encoding.UTF8.GetBytes("名前=テスト;🌟=⭐");
        var unicodeBody = Encoding.UTF8.GetBytes("こんにちは世界! 🎉 Hello World! 🌍");
        var message = new Message(MessageType.Publish, 1UL, 0L, unicodeProperties, unicodeBody);
        _messagesToDispose.Add(message);

        var buffer = new byte[message.TotalSize];

        // Act
        message.Serialize(buffer);
        var (deserialized, _) = Message.Deserialize(buffer);
        _messagesToDispose.Add(deserialized);

        // Assert
        deserialized.Properties.ToArray().Should().BeEquivalentTo(unicodeProperties);
        deserialized.Body.ToArray().Should().BeEquivalentTo(unicodeBody);

        // Verify the strings can be decoded back
        var decodedProperties = Encoding.UTF8.GetString(deserialized.Properties.Span);
        var decodedBody = Encoding.UTF8.GetString(deserialized.Body.Span);
        decodedProperties.Should().Be("名前=テスト;🌟=⭐");
        decodedBody.Should().Be("こんにちは世界! 🎉 Hello World! 🌍");
    }

    public void Dispose()
    {
        foreach (var message in _messagesToDispose)
        {
            message.Dispose();
        }
        _messagesToDispose.Clear();
    }
}
