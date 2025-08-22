using System.Buffers;
using System.Collections.Concurrent;
using System.Text;

namespace QueueServer.Core.Models;

/// <summary>
/// High-performance message structure with zero-copy design
/// Binary format: [4 bytes: total_length][4 bytes: message_type][8 bytes: sequence_number]
/// [8 bytes: timestamp][4 bytes: properties_length][properties_data][4 bytes: body_length][body_data]
/// </summary>
public readonly struct Message : IDisposable
{
    /// <summary>
    /// Fixed header size in bytes
    /// </summary>
    public const int HeaderSize = 32;
    
    /// <summary>
    /// Type of the message
    /// </summary>
    public readonly MessageType MessageType;
    
    /// <summary>
    /// Sequence number for ordering
    /// </summary>
    public readonly ulong SequenceNumber;
    
    /// <summary>
    /// Unix timestamp in milliseconds
    /// </summary>
    public readonly long Timestamp;
    
    /// <summary>
    /// Message properties as binary data
    /// </summary>
    public readonly ReadOnlyMemory<byte> Properties;
    
    /// <summary>
    /// Message body as binary data
    /// </summary>
    public readonly ReadOnlyMemory<byte> Body;
    
    private readonly IMemoryOwner<byte>? _propertyOwner;
    private readonly IMemoryOwner<byte>? _bodyOwner;

    /// <summary>
    /// Constructs a new message with the specified parameters
    /// </summary>
    public Message(
        MessageType messageType,
        ulong sequenceNumber,
        long timestamp,
        ReadOnlyMemory<byte> properties,
        ReadOnlyMemory<byte> body,
        IMemoryOwner<byte>? propertyOwner = null,
        IMemoryOwner<byte>? bodyOwner = null)
    {
        MessageType = messageType;
        SequenceNumber = sequenceNumber;
        Timestamp = timestamp;
        Properties = properties;
        Body = body;
        _propertyOwner = propertyOwner;
        _bodyOwner = bodyOwner;
    }

    /// <summary>
    /// Total serialized size of the message
    /// </summary>
    public int TotalSize => HeaderSize + Properties.Length + Body.Length;

    /// <summary>
    /// Serialize message to a buffer using span-based operations
    /// </summary>
    public void Serialize(Span<byte> buffer)
    {
        if (buffer.Length < TotalSize)
            throw new ArgumentException($"Buffer too small. Required: {TotalSize}, Available: {buffer.Length}");

        var writer = new SpanWriter(buffer);
        
        // Write header
        writer.WriteInt32(TotalSize);
        writer.WriteUInt32((uint)MessageType);
        writer.WriteUInt64(SequenceNumber);
        writer.WriteInt64(Timestamp);
        
        // Write properties
        writer.WriteInt32(Properties.Length);
        Properties.Span.CopyTo(writer.GetSpan(Properties.Length));
        writer.Advance(Properties.Length);
        
        // Write body
        writer.WriteInt32(Body.Length);
        Body.Span.CopyTo(writer.GetSpan(Body.Length));
        writer.Advance(Body.Length);
    }

    /// <summary>
    /// Deserialize message from memory with zero-copy when possible
    /// </summary>
    public static (Message Message, int BytesRead) Deserialize(ReadOnlyMemory<byte> buffer)
    {
        if (buffer.Length < HeaderSize)
            throw new ArgumentException("Buffer too small for message header");

        var reader = new SpanReader(buffer.Span);
        
        // Read header
        var totalLength = reader.ReadInt32();
        var messageType = (MessageType)reader.ReadUInt32();
        var sequenceNumber = reader.ReadUInt64();
        var timestamp = reader.ReadInt64();
        
        if (buffer.Length < totalLength)
            throw new ArgumentException($"Buffer too small for complete message. Required: {totalLength}, Available: {buffer.Length}");
        
        // Read properties
        var propertiesLength = reader.ReadInt32();
        var properties = buffer.Slice(reader.Position, propertiesLength);
        reader.Advance(propertiesLength);
        
        // Read body
        var bodyLength = reader.ReadInt32();
        var body = buffer.Slice(reader.Position, bodyLength);
        reader.Advance(bodyLength);
        
        var message = new Message(messageType, sequenceNumber, timestamp, properties, body);
        return (message, totalLength);
    }

    /// <summary>
    /// Dispose managed resources
    /// </summary>
    public void Dispose()
    {
        _propertyOwner?.Dispose();
        _bodyOwner?.Dispose();
    }
}

/// <summary>
/// High-performance message builder with pooled memory
/// </summary>
public sealed class MessageBuilder : IDisposable
{
    private readonly MessageType _messageType;
    private ulong _sequenceNumber;
    private long _timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    
    private IMemoryOwner<byte>? _propertyOwner;
    private Memory<byte> _propertyBuffer;
    private int _propertyLength;
    
    private IMemoryOwner<byte>? _bodyOwner;
    private Memory<byte> _bodyBuffer;
    private int _bodyLength;
    
    private readonly ConcurrentDictionary<string, string> _properties = new();

    /// <summary>
    /// Initialize a new message builder
    /// </summary>
    public MessageBuilder(MessageType messageType)
    {
        _messageType = messageType;
    }

    /// <summary>
    /// Set sequence number
    /// </summary>
    public MessageBuilder WithSequenceNumber(ulong sequenceNumber)
    {
        _sequenceNumber = sequenceNumber;
        return this;
    }

    /// <summary>
    /// Set timestamp
    /// </summary>
    public MessageBuilder WithTimestamp(long timestamp)
    {
        _timestamp = timestamp;
        return this;
    }

    /// <summary>
    /// Add a property
    /// </summary>
    public MessageBuilder WithProperty(string key, string value)
    {
        _properties[key] = value;
        return this;
    }

    /// <summary>
    /// Set message body from byte span
    /// </summary>
    public MessageBuilder WithBody(ReadOnlySpan<byte> body)
    {
        _bodyOwner?.Dispose();
        _bodyOwner = MemoryPool<byte>.Shared.Rent(body.Length);
        _bodyBuffer = _bodyOwner.Memory[..body.Length];
        body.CopyTo(_bodyBuffer.Span);
        _bodyLength = body.Length;
        return this;
    }

    /// <summary>
    /// Set message body from text
    /// </summary>
    public MessageBuilder WithTextBody(string text)
    {
        var bytes = Encoding.UTF8.GetBytes(text);
        return WithBody(bytes);
    }

    /// <summary>
    /// Build the final message
    /// </summary>
    public Message Build()
    {
        SerializeProperties();
        
        return new Message(
            _messageType,
            _sequenceNumber,
            _timestamp,
            _propertyBuffer[.._propertyLength],
            _bodyBuffer[.._bodyLength],
            _propertyOwner,
            _bodyOwner);
    }

    private void SerializeProperties()
    {
        if (_properties.IsEmpty)
        {
            _propertyLength = 0;
            return;
        }

        // Estimate size needed for properties
        var estimatedSize = _properties.Sum(kv => Encoding.UTF8.GetByteCount(kv.Key) + 
                                                  Encoding.UTF8.GetByteCount(kv.Value) + 2); // +2 for '=' and '\0'
        
        _propertyOwner?.Dispose();
        _propertyOwner = MemoryPool<byte>.Shared.Rent(estimatedSize);
        _propertyBuffer = _propertyOwner.Memory;
        
        var span = _propertyBuffer.Span;
        var position = 0;
        
        foreach (var (key, value) in _properties)
        {
            var keyBytes = Encoding.UTF8.GetBytes(key);
            var valueBytes = Encoding.UTF8.GetBytes(value);
            
            keyBytes.CopyTo(span[position..]);
            position += keyBytes.Length;
            
            span[position++] = (byte)'=';
            
            valueBytes.CopyTo(span[position..]);
            position += valueBytes.Length;
            
            span[position++] = 0; // Null terminator
        }
        
        _propertyLength = position;
    }

    /// <summary>
    /// Dispose managed resources
    /// </summary>
    public void Dispose()
    {
        _propertyOwner?.Dispose();
        _bodyOwner?.Dispose();
        _properties.Clear();
    }
}

/// <summary>
/// High-performance span-based writer
/// </summary>
internal ref struct SpanWriter
{
    private Span<byte> _span;
    private int _position;

    public SpanWriter(Span<byte> span)
    {
        _span = span;
        _position = 0;
    }

    public void WriteInt32(int value)
    {
        if (_position + 4 > _span.Length)
            throw new InvalidOperationException("Not enough space in buffer");
        
        BitConverter.TryWriteBytes(_span[_position..], value);
        _position += 4;
    }

    public void WriteUInt32(uint value)
    {
        if (_position + 4 > _span.Length)
            throw new InvalidOperationException("Not enough space in buffer");
        
        BitConverter.TryWriteBytes(_span[_position..], value);
        _position += 4;
    }

    public void WriteUInt64(ulong value)
    {
        if (_position + 8 > _span.Length)
            throw new InvalidOperationException("Not enough space in buffer");
        
        BitConverter.TryWriteBytes(_span[_position..], value);
        _position += 8;
    }

    public void WriteInt64(long value)
    {
        if (_position + 8 > _span.Length)
            throw new InvalidOperationException("Not enough space in buffer");
        
        BitConverter.TryWriteBytes(_span[_position..], value);
        _position += 8;
    }

    public Span<byte> GetSpan(int length)
    {
        if (_position + length > _span.Length)
            throw new InvalidOperationException("Not enough space in buffer");
        
        return _span.Slice(_position, length);
    }

    public void Advance(int count)
    {
        _position += count;
    }
}

/// <summary>
/// High-performance span-based reader
/// </summary>
internal ref struct SpanReader
{
    private ReadOnlySpan<byte> _span;
    private int _position;

    public int Position => _position;

    public SpanReader(ReadOnlySpan<byte> span)
    {
        _span = span;
        _position = 0;
    }

    public int ReadInt32()
    {
        if (_position + 4 > _span.Length)
            throw new InvalidOperationException("Not enough data in buffer");
        
        var value = BitConverter.ToInt32(_span[_position..]);
        _position += 4;
        return value;
    }

    public uint ReadUInt32()
    {
        if (_position + 4 > _span.Length)
            throw new InvalidOperationException("Not enough data in buffer");
        
        var value = BitConverter.ToUInt32(_span[_position..]);
        _position += 4;
        return value;
    }

    public ulong ReadUInt64()
    {
        if (_position + 8 > _span.Length)
            throw new InvalidOperationException("Not enough data in buffer");
        
        var value = BitConverter.ToUInt64(_span[_position..]);
        _position += 8;
        return value;
    }

    public long ReadInt64()
    {
        if (_position + 8 > _span.Length)
            throw new InvalidOperationException("Not enough data in buffer");
        
        var value = BitConverter.ToInt64(_span[_position..]);
        _position += 8;
        return value;
    }

    public void Advance(int count)
    {
        _position += count;
    }
}
