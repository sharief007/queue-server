#pragma warning disable CS1998 // Async method lacks 'await' operators
using System.Buffers;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using QueueServer.Core.Configuration;
using QueueServer.Core.Models;

namespace QueueServer.Core.Network;

/// <summary>
/// High-performance client connection with zero-copy I/O
/// </summary>
public sealed class ClientConnection : IAsyncDisposable
{
    private readonly Socket _socket;
    private readonly string _connectionId;
    private readonly EndPoint _remoteEndPoint;
    private readonly ServerConfiguration _config;
    private readonly CancellationTokenSource _cancellationTokenSource = new();
    private readonly ArrayPool<byte> _bufferPool = ArrayPool<byte>.Shared;
    
    // Connection state
    private ConnectionState _state = ConnectionState.Connected;
    private DateTime _lastActivity = DateTime.UtcNow;
    private DateTime _lastHeartbeat = DateTime.UtcNow;
    
    // Send/receive buffers
    private readonly object _sendLock = new();
    private readonly SemaphoreSlim _receiveSemaphore = new(1, 1);
    
    private volatile bool _disposed;

    public ClientConnection(Socket socket, string connectionId, ServerConfiguration config)
    {
        _socket = socket;
        _connectionId = connectionId;
        _remoteEndPoint = socket.RemoteEndPoint ?? new IPEndPoint(IPAddress.None, 0);
        _config = config;
        
        // Configure socket options for performance
        _socket.NoDelay = true;
        _socket.ReceiveBufferSize = _config.ReceiveBufferSize;
        _socket.SendBufferSize = _config.SendBufferSize;
        _socket.ReceiveTimeout = (int)_config.ConnectionTimeout.TotalMilliseconds;
        _socket.SendTimeout = (int)_config.ConnectionTimeout.TotalMilliseconds;
    }

    public string ConnectionId => _connectionId;
    public EndPoint RemoteEndPoint => _remoteEndPoint;
    public ConnectionState State => _state;
    public DateTime LastActivity => _lastActivity;
    public DateTime LastHeartbeat => _lastHeartbeat;
    public bool IsConnected => _state == ConnectionState.Connected && _socket.Connected;

    /// <summary>
    /// Send a message asynchronously with zero-copy when possible
    /// </summary>
    public async Task<bool> SendMessageAsync(Message message, CancellationToken cancellationToken = default)
    {
        if (_disposed || !IsConnected) return false;

        try
        {
            var buffer = _bufferPool.Rent(message.TotalSize);
            try
            {
                var messageSpan = buffer.AsMemory(0, message.TotalSize);
                message.Serialize(messageSpan.Span);

                lock (_sendLock)
                {
                    var sent = _socket.Send(messageSpan.Span, SocketFlags.None);
                    if (sent != message.TotalSize)
                    {
                        return false;
                    }
                }

                _lastActivity = DateTime.UtcNow;
                return true;
            }
            finally
            {
                _bufferPool.Return(buffer);
            }
        }
        catch (Exception)
        {
            await DisconnectAsync();
            return false;
        }
    }

    /// <summary>
    /// Receive a message asynchronously
    /// </summary>
    public async Task<(Message? Message, bool Success)> ReceiveMessageAsync(CancellationToken cancellationToken = default)
    {
        if (_disposed || !IsConnected) return (null, false);

        await _receiveSemaphore.WaitAsync(cancellationToken);
        try
        {
            // First, read the message header to get total length
            var headerBuffer = _bufferPool.Rent(Message.HeaderSize);
            try
            {
                var headerMemory = headerBuffer.AsMemory(0, Message.HeaderSize);
                
                var bytesReceived = await ReceiveExactAsync(headerMemory, cancellationToken);
                if (bytesReceived != Message.HeaderSize)
                {
                    return (null, false);
                }

                // Parse total length from header
                var totalLength = BitConverter.ToInt32(headerMemory.Span[0..4]);
                if (totalLength < Message.HeaderSize || totalLength > _config.ReceiveBufferSize)
                {
                    return (null, false);
                }

                // Read the complete message
                var messageBuffer = _bufferPool.Rent(totalLength);
                try
                {
                    var messageMemory = messageBuffer.AsMemory(0, totalLength);
                    
                    // Copy header to message buffer
                    headerMemory.CopyTo(messageMemory);
                    
                    // Read remaining data
                    var remainingLength = totalLength - Message.HeaderSize;
                    if (remainingLength > 0)
                    {
                        var remainingMemory = messageMemory[Message.HeaderSize..];
                        bytesReceived = await ReceiveExactAsync(remainingMemory, cancellationToken);
                        if (bytesReceived != remainingLength)
                        {
                            return (null, false);
                        }
                    }

                    // Deserialize message
                    var (deserializedMessage, _) = Message.Deserialize(messageMemory);
                    _lastActivity = DateTime.UtcNow;
                    return (deserializedMessage, true);
                }
                finally
                {
                    _bufferPool.Return(messageBuffer);
                }
            }
            finally
            {
                _bufferPool.Return(headerBuffer);
            }
        }
        catch (Exception)
        {
            await DisconnectAsync();
            return (null, false);
        }
        finally
        {
            _receiveSemaphore.Release();
        }
    }

    /// <summary>
    /// Receive exact number of bytes
    /// </summary>
    private async Task<int> ReceiveExactAsync(Memory<byte> buffer, CancellationToken cancellationToken)
    {
        var totalReceived = 0;
        var remaining = buffer.Length;
        
        while (remaining > 0 && !cancellationToken.IsCancellationRequested)
        {
            var received = await Task.Factory.FromAsync(
                (callback, state) => _socket.BeginReceive(buffer.Span[totalReceived..].ToArray(), 0, remaining, SocketFlags.None, callback, state),
                _socket.EndReceive,
                null);
                
            if (received == 0)
            {
                break; // Connection closed
            }
            
            totalReceived += received;
            remaining -= received;
        }
        
        return totalReceived;
    }

    /// <summary>
    /// Update heartbeat timestamp
    /// </summary>
    public void UpdateHeartbeat()
    {
        _lastHeartbeat = DateTime.UtcNow;
        _lastActivity = DateTime.UtcNow;
    }

    /// <summary>
    /// Check if connection is healthy
    /// </summary>
    public bool IsHealthy()
    {
        if (_disposed || !IsConnected) return false;
        
        var now = DateTime.UtcNow;
        var heartbeatTimeout = _lastHeartbeat.Add(_config.HeartbeatInterval.Add(_config.HeartbeatInterval));
        
        return now < heartbeatTimeout;
    }

    /// <summary>
    /// Disconnect the client
    /// </summary>
    public async Task DisconnectAsync()
    {
        if (_state == ConnectionState.Disconnected) return;
        
        _state = ConnectionState.Disconnecting;
        
        try
        {
            if (_socket.Connected)
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
            _state = ConnectionState.Disconnected;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        await DisconnectAsync();
        
        _cancellationTokenSource.Cancel();
        _socket.Dispose();
        _cancellationTokenSource.Dispose();
        _receiveSemaphore.Dispose();
    }
}

/// <summary>
/// High-performance TCP server with connection pooling
/// </summary>
public sealed class TcpServer : IAsyncDisposable
{
    private readonly ServerConfiguration _config;
    private readonly Socket _listenSocket;
    private readonly CancellationTokenSource _cancellationTokenSource = new();
    private readonly ConcurrentDictionary<string, ClientConnection> _connections = new();
    private readonly SemaphoreSlim _connectionSemaphore;
    private readonly Timer _healthCheckTimer;
    
    private volatile bool _isRunning;
    private volatile bool _disposed;
    private Task? _acceptTask;

    public TcpServer(ServerConfiguration config)
    {
        _config = config;
        _connectionSemaphore = new SemaphoreSlim(_config.MaxConnections, _config.MaxConnections);
        
        // Create and configure listen socket
        _listenSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        _listenSocket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
        _listenSocket.NoDelay = true;
        
        // Setup health check timer
        _healthCheckTimer = new Timer(CheckConnectionHealth, null, 
            TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(30));
    }

    public bool IsRunning => _isRunning;
    public int ConnectionCount => _connections.Count;

    /// <summary>
    /// Event raised when a new client connects
    /// </summary>
    public event Func<ClientConnection, Task>? ClientConnected;

    /// <summary>
    /// Event raised when a client disconnects
    /// </summary>
    public event Func<ClientConnection, Task>? ClientDisconnected;

    /// <summary>
    /// Start the server
    /// </summary>
    public async Task StartAsync()
    {
        if (_isRunning) return;
        
        var endpoint = new IPEndPoint(IPAddress.Parse(_config.Host), _config.Port);
        _listenSocket.Bind(endpoint);
        _listenSocket.Listen(_config.MaxConnections);
        
        _isRunning = true;
        _acceptTask = Task.Run(AcceptClients, _cancellationTokenSource.Token);
        
        Console.WriteLine($"TCP Server started on {endpoint}");
    }

    /// <summary>
    /// Stop the server
    /// </summary>
    public async Task StopAsync()
    {
        if (!_isRunning) return;
        
        _isRunning = false;
        _cancellationTokenSource.Cancel();
        
        try
        {
            _listenSocket.Close();
            if (_acceptTask != null)
            {
                await _acceptTask;
            }
        }
        catch (OperationCanceledException)
        {
            // Expected
        }
        
        // Disconnect all clients
        await Task.WhenAll(_connections.Values.Select(conn => conn.DisposeAsync().AsTask()));
        _connections.Clear();
        
        Console.WriteLine("TCP Server stopped");
    }

    /// <summary>
    /// Get connection by ID
    /// </summary>
    public ClientConnection? GetConnection(string connectionId)
    {
        return _connections.GetValueOrDefault(connectionId);
    }

    /// <summary>
    /// Remove a connection
    /// </summary>
    public async Task<bool> RemoveConnectionAsync(string connectionId)
    {
        if (_connections.TryRemove(connectionId, out var connection))
        {
            await connection.DisposeAsync();
            _connectionSemaphore.Release();
            
            try
            {
                if (ClientDisconnected != null)
                {
                    await ClientDisconnected(connection);
                }
            }
            catch
            {
                // Ignore errors in event handlers
            }
            
            return true;
        }
        
        return false;
    }

    /// <summary>
    /// Accept incoming client connections
    /// </summary>
    private async Task AcceptClients()
    {
        while (_isRunning && !_cancellationTokenSource.Token.IsCancellationRequested)
        {
            try
            {
                // Wait for connection slot
                await _connectionSemaphore.WaitAsync(_cancellationTokenSource.Token);
                
                // Accept client
                var clientSocket = await Task.Factory.FromAsync(
                    _listenSocket.BeginAccept,
                    _listenSocket.EndAccept,
                    null);
                
                // Create connection
                var connectionId = Guid.NewGuid().ToString();
                var connection = new ClientConnection(clientSocket, connectionId, _config);
                
                if (_connections.TryAdd(connectionId, connection))
                {
                    // Handle client connection
                    _ = Task.Run(async () =>
                    {
                        try
                        {
                            if (ClientConnected != null)
                            {
                                await ClientConnected(connection);
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Error handling client connection: {ex.Message}");
                            await RemoveConnectionAsync(connectionId);
                        }
                    }, _cancellationTokenSource.Token);
                }
                else
                {
                    await connection.DisposeAsync();
                    _connectionSemaphore.Release();
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error accepting client: {ex.Message}");
                _connectionSemaphore.Release();
            }
        }
    }

    /// <summary>
    /// Check connection health and remove unhealthy connections
    /// </summary>
    private void CheckConnectionHealth(object? state)
    {
        if (_disposed || !_isRunning) return;
        
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
            _ = Task.Run(() => RemoveConnectionAsync(connectionId));
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        await StopAsync();
        
        _healthCheckTimer?.Dispose();
        _listenSocket?.Dispose();
        _cancellationTokenSource.Dispose();
        _connectionSemaphore.Dispose();
    }
}
