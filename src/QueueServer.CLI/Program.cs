using QueueServer.Client;

namespace QueueServer.CLI;

/// <summary>
/// Command-line interface for QueueServer
/// </summary>
internal static class Program
{
    public static async Task<int> Main(string[] args)
    {
        if (args.Length == 0)
        {
            ShowUsage();
            return 1;
        }

        var command = args[0].ToLowerInvariant();
        var host = GetArgument(args, "--host") ?? "127.0.0.1";
        var portStr = GetArgument(args, "--port") ?? "9999";

        if (!int.TryParse(portStr, out var port))
        {
            Console.WriteLine("Invalid port number");
            return 1;
        }

        try
        {
            await using var client = new BrokerClient(host, port);
            await client.ConnectAsync();

            return command switch
            {
                "create-topic" => await CreateTopic(client, args),
                "create-subscription" => await CreateSubscription(client, args),
                "delete-subscription" => await DeleteSubscription(client, args),
                "publish" => await Publish(client, args),
                "subscribe" => await Subscribe(client, args),
                "benchmark" => await Benchmark(client, args),
                _ => InvalidCommand(),
            };
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
            return 1;
        }
    }

    private static int InvalidCommand()
    {
        ShowUsage();
        return 1;
    }

    private static void ShowUsage()
    {
        Console.WriteLine("QueueServer CLI - High Performance Message Broker Client");
        Console.WriteLine("=======================================================");
        Console.WriteLine();
        Console.WriteLine("Usage:");
        Console.WriteLine("  qserver <command> [options] [arguments]");
        Console.WriteLine();
        Console.WriteLine("Commands:");
        Console.WriteLine("  create-topic <topic-name> [--retention-hours <hours>]");
        Console.WriteLine(
            "  create-subscription <subscription-id> <topic-name> [--start-offset <offset>]"
        );
        Console.WriteLine("  delete-subscription <subscription-id>");
        Console.WriteLine("  publish <topic-name> <message> [--property key=value]...");
        Console.WriteLine("  subscribe <subscription-id> [--count <max-messages>]");
        Console.WriteLine("  benchmark [--messages <count>] [--topic <name>] [--size <bytes>]");
        Console.WriteLine();
        Console.WriteLine("Global Options:");
        Console.WriteLine("  --host <hostname>     Broker hostname (default: 127.0.0.1)");
        Console.WriteLine("  --port <port>         Broker port (default: 9999)");
        Console.WriteLine();
        Console.WriteLine("Examples:");
        Console.WriteLine("  qserver create-topic events");
        Console.WriteLine("  qserver create-subscription my-group events --start-offset 0");
        Console.WriteLine("  qserver publish events \"Hello World\" --property sender=cli");
        Console.WriteLine("  qserver subscribe my-group --count 10");
        Console.WriteLine("  qserver benchmark --messages 10000 --topic perf-test --size 1024");
    }

    private static async Task<int> CreateTopic(BrokerClient client, string[] args)
    {
        if (args.Length < 2)
        {
            Console.WriteLine("Usage: create-topic <topic-name> [--retention-hours <hours>]");
            return 1;
        }

        var topicName = args[1];
        var retentionStr = GetArgument(args, "--retention-hours") ?? "24";

        if (!int.TryParse(retentionStr, out var retentionHours))
        {
            Console.WriteLine("Invalid retention hours");
            return 1;
        }

        var success = await client.CreateTopicAsync(topicName, retentionHours);
        if (success)
        {
            Console.WriteLine($"Topic '{topicName}' created successfully");
            return 0;
        }
        else
        {
            Console.WriteLine($"Failed to create topic '{topicName}'");
            return 1;
        }
    }

    private static async Task<int> CreateSubscription(BrokerClient client, string[] args)
    {
        if (args.Length < 3)
        {
            Console.WriteLine(
                "Usage: create-subscription <subscription-id> <topic-name> [--start-offset <offset>]"
            );
            return 1;
        }

        var subscriptionId = args[1];
        var topicName = args[2];
        var offsetStr = GetArgument(args, "--start-offset") ?? "0";

        if (!ulong.TryParse(offsetStr, out var startOffset))
        {
            Console.WriteLine("Invalid start offset");
            return 1;
        }

        var success = await client.CreateSubscriptionAsync(subscriptionId, topicName, startOffset);
        if (success)
        {
            Console.WriteLine($"Subscription '{subscriptionId}' created for topic '{topicName}'");
            return 0;
        }
        else
        {
            Console.WriteLine($"Failed to create subscription '{subscriptionId}'");
            return 1;
        }
    }

    private static async Task<int> DeleteSubscription(BrokerClient client, string[] args)
    {
        if (args.Length < 2)
        {
            Console.WriteLine("Usage: delete-subscription <subscription-id>");
            return 1;
        }

        var subscriptionId = args[1];
        var success = await client.DeleteSubscriptionAsync(subscriptionId);
        if (success)
        {
            Console.WriteLine($"Subscription '{subscriptionId}' deleted successfully");
            return 0;
        }
        else
        {
            Console.WriteLine($"Failed to delete subscription '{subscriptionId}'");
            return 1;
        }
    }

    private static async Task<int> Publish(BrokerClient client, string[] args)
    {
        if (args.Length < 3)
        {
            Console.WriteLine("Usage: publish <topic-name> <message> [--property key=value]...");
            return 1;
        }

        var topicName = args[1];
        var message = args[2];
        var properties = ParseProperties(args);

        var success = await client.PublishTextAsync(topicName, message, properties);
        if (success)
        {
            Console.WriteLine($"Message published to topic '{topicName}'");
            return 0;
        }
        else
        {
            Console.WriteLine($"Failed to publish message to topic '{topicName}'");
            return 1;
        }
    }

    private static async Task<int> Subscribe(BrokerClient client, string[] args)
    {
        if (args.Length < 2)
        {
            Console.WriteLine("Usage: subscribe <subscription-id> [--count <max-messages>]");
            return 1;
        }

        var subscriptionId = args[1];
        var countStr = GetArgument(args, "--count");
        var maxMessages =
            countStr != null && int.TryParse(countStr, out var count) ? count : int.MaxValue;

        var messagesReceived = 0;
        var cts = new CancellationTokenSource();

        // Handle Ctrl+C
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };

        var success = await client.SubscribeAsync(
            subscriptionId,
            message =>
            {
                var body = System.Text.Encoding.UTF8.GetString(message.Body.Span);
                var timestamp = DateTimeOffset.FromUnixTimeMilliseconds(message.Timestamp);

                Console.WriteLine(
                    $"[{timestamp:yyyy-MM-dd HH:mm:ss.fff}] Offset {message.SequenceNumber}: {body}"
                );

                if (++messagesReceived >= maxMessages)
                {
                    cts.Cancel();
                }

                return Task.CompletedTask;
            }
        );

        if (!success)
        {
            Console.WriteLine($"Failed to subscribe to '{subscriptionId}'");
            return 1;
        }

        Console.WriteLine($"Subscribed to '{subscriptionId}'. Press Ctrl+C to stop...");

        try
        {
            await Task.Delay(-1, cts.Token);
        }
        catch (OperationCanceledException)
        {
            // Expected
        }

        await client.UnsubscribeAsync(subscriptionId);
        Console.WriteLine($"\nReceived {messagesReceived} messages");
        return 0;
    }

    private static async Task<int> Benchmark(BrokerClient client, string[] args)
    {
        var messageCountStr = GetArgument(args, "--messages") ?? "1000";
        var topicName = GetArgument(args, "--topic") ?? "benchmark";
        var messageSizeStr = GetArgument(args, "--size") ?? "64";

        if (
            !int.TryParse(messageCountStr, out var messageCount)
            || !int.TryParse(messageSizeStr, out var messageSize)
        )
        {
            Console.WriteLine("Invalid benchmark parameters");
            return 1;
        }

        // Create topic
        await client.CreateTopicAsync(topicName);

        // Generate test message
        var testMessage = new string('A', messageSize);

        Console.WriteLine(
            $"Starting benchmark: {messageCount} messages of {messageSize} bytes each"
        );
        Console.WriteLine($"Topic: {topicName}");

        var startTime = DateTime.UtcNow;
        var successCount = 0;

        for (var i = 0; i < messageCount; i++)
        {
            var success = await client.PublishTextAsync(
                topicName,
                testMessage,
                new Dictionary<string, string>
                {
                    ["message_id"] = i.ToString(),
                    ["timestamp"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString(),
                }
            );

            if (success)
                successCount++;

            if ((i + 1) % 100 == 0)
            {
                Console.Write($"\rProgress: {i + 1}/{messageCount} ({successCount} successful)");
            }
        }

        var endTime = DateTime.UtcNow;
        var duration = endTime - startTime;
        var messagesPerSecond = messageCount / duration.TotalSeconds;
        var bytesPerSecond = successCount * messageSize / duration.TotalSeconds;

        Console.WriteLine($"\n\nBenchmark Results:");
        Console.WriteLine($"Duration: {duration.TotalMilliseconds:F2} ms");
        Console.WriteLine($"Messages sent: {messageCount}");
        Console.WriteLine($"Successful: {successCount}");
        Console.WriteLine($"Failed: {messageCount - successCount}");
        Console.WriteLine($"Throughput: {messagesPerSecond:F2} messages/sec");
        Console.WriteLine($"Bandwidth: {bytesPerSecond / 1024 / 1024:F2} MB/sec");

        return 0;
    }

    private static string? GetArgument(string[] args, string name)
    {
        for (var i = 0; i < args.Length - 1; i++)
        {
            if (args[i].Equals(name, StringComparison.OrdinalIgnoreCase))
            {
                return args[i + 1];
            }
        }
        return null;
    }

    private static Dictionary<string, string>? ParseProperties(string[] args)
    {
        var properties = new Dictionary<string, string>();

        for (var i = 0; i < args.Length - 1; i++)
        {
            if (args[i].Equals("--property", StringComparison.OrdinalIgnoreCase))
            {
                var property = args[i + 1];
                var equalPos = property.IndexOf('=');
                if (equalPos > 0 && equalPos < property.Length - 1)
                {
                    var key = property[..equalPos];
                    var value = property[(equalPos + 1)..];
                    properties[key] = value;
                }
            }
        }

        return properties.Count > 0 ? properties : null;
    }
}
