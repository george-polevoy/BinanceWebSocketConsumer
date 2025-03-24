using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Net.WebSockets;
using System.Text.Json;
using System.Threading.Channels;
using Serilog;
using Serilog.Formatting.Json;

var builder = WebApplication.CreateBuilder(args);

Log.Logger = new LoggerConfiguration()
    .Enrich.FromLogContext()
    .ReadFrom.Configuration(builder.Configuration)
    //.WriteTo.Console(theme: AnsiConsoleTheme.Code, applyThemeToRedirectedOutput: true)
    .WriteTo.Console(new JsonFormatter())
    .CreateLogger();

Log.Information("Starting up");

builder.Services.AddLogging(
    loggingBuilder =>
    {
        loggingBuilder.ClearProviders();
        loggingBuilder.AddConfiguration(builder.Configuration.GetRequiredSection("Logging"));
        loggingBuilder.AddSerilog(Log.Logger);
    });
builder.Services.Configure<ConsoleLifetimeOptions>(o => o.SuppressStatusMessages = true);

builder.Services.AddHostedService<WsWorker>();

var app = builder.Build();

app.Run();

public class WsWorker(ILogger<WsWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await RunExperimentalSocketConnection(stoppingToken);
    }

    private async Task RunExperimentalSocketConnection(CancellationToken cancellationToken)
    {
        var binanceCombinedStreamUrl = "wss://fstream.binance.com/stream?streams=";

        var assets = new[]
        {
            "BTC", "ETH", "XRP", "BNB", "SOL", "USDC", "DOGE", "ADA", "TRX",
            "LINK", "XLM", "AVAX", "LTC", "SUI", "TON", "SHIB", "LEO", "HBAR", "HYPE",
            "DOT", "OM", "BCH", "BGB", "USDe", "UNI", "DAI", "XMR", "PEPE", "AAVE",
            "ONDO", "NEAR", "MNT", "TRUMP", "APT", "ICP", "TAO", "ETC", "OKB", "KAS",
            "POL", "VET", "CRO", "ALGO", "RENDER", "FIL", "ARB", "FDUSD", "GT", "JUP",
            "ATOM", "OP", "FET", "S", "LDO", "TIA", "DEXE", "KCS", "INJ", "XDC", "STX",
            "ENA", "IMX", "GRT", "RAY", "THETA", "MOVE", "BONK", "WLD", "FLR", "QNT",
            "JASMY", "SEI", "EOS", "ENS", "MKR", "SAND", "FLOKI", "XTZ", "NEXO", "BTT",
            "GALA", "IOTA", "FLOW", "JTO", "BSV", "RON", "CAKE", "NEO", "KAIA", "PYTH",
            "VIRTUAL", "XAUt", "FTT", "XCN", "MELANIA", "AXS", "CRV", "PYUSD", "WIF"
        };

        var assetStreamSpecs = new[] { "trade", "depth@500ms", "bookTicker", "aggTrade", "markPrice@1s", "forceOrder" };

        var assetStreams = assets
            .Take(10)
            .SelectMany(asset => assetStreamSpecs
                .Select(streamSpec => $"{asset.ToLower()}usdt@{streamSpec}")).ToArray();

        var allMarketStreams = new[] { "!contractInfo" };

        var query = string.Join("/", assetStreams.Concat(allMarketStreams));

        var streamUrl = $"{binanceCombinedStreamUrl}{query}";

        var deadLetterCounter = 0L;
        
        async Task DeadLetterReporter()
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
                }
                catch (OperationCanceledException)
                {
                }
                var count = Interlocked.Exchange(ref deadLetterCounter, 0);
                if (count > 0)
                {
                    logger.LogInformation("{where} {what} {count}", nameof(RunExperimentalSocketConnection),
                        "DeadLetter", count);
                }
            }
        }
        
        var deadLetterReporter = DeadLetterReporter();

        // Must be set slightly larger than 20KB (exceeding the WebSocket implementation's frame buffer size)
        // to prevent message loss during high-throughput scenarios. 
        // Allocates 10MB of memory capacity for buffering unprocessed incoming messages.
        const double requiredBufferSizeBytes = 10_000_000.0;
        //const double requiredBufferSizeBytes = 10_00.0;
        const double characteristicMessageSizeBytes = 150.0;
        const int bufferSize = (int)(requiredBufferSizeBytes / characteristicMessageSizeBytes);
        var channel = Channel.CreateBounded<Message>(new BoundedChannelOptions(bufferSize)
            {
                SingleWriter = true,
                SingleReader = false,
                FullMode = BoundedChannelFullMode.DropOldest
            },
            message =>
            {
                ArrayPool<byte>.Shared.Return(message.Buffer);
                Interlocked.Increment(ref deadLetterCounter);
            }
            );

        var writingTask = ConsumeChannel(channel.Reader);
        var readingTask = RunWebSocketReadingWithReconnect(streamUrl, channel.Writer, cancellationToken);

        await readingTask;
        channel.Writer.Complete();

        await writingTask;
        await deadLetterReporter;
    }

    async Task ConsumeChannel(ChannelReader<Message> channelReader)
    {
        var count = 0L;
        await foreach (var message in channelReader.ReadAllAsync())
        {
            var (stream, data) = PartiallyParseMessage(message.Contents);

            count++;
            if (count % 1000 == 0)
            {
                logger.LogInformation("{where} {what} {channel} {size}", nameof(ConsumeChannel), "Received", stream,
                    data.Length);
                logger.LogInformation("{where} {what} {count} {allocated}", nameof(ConsumeChannel), "Processed", count,
                    GC.GetTotalAllocatedBytes());
            }

            ArrayPool<byte>.Shared.Return(message.Buffer);
        }

        logger.LogInformation("{where} {what}", nameof(ConsumeChannel), "Completed");
    }

    async Task RunWebSocketReadingWithReconnect(string streamUrl, ChannelWriter<Message> channelWriter,
        CancellationToken cancellationToken)
    {
        const double maxDelaySeconds = 3;
        while (!cancellationToken.IsCancellationRequested)
        {
            var sw = Stopwatch.StartNew();
            try
            {
                await RunOnSocket(streamUrl, channelWriter, cancellationToken);
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
            {
                logger.LogError(ex, "{where} {what}", nameof(RunWebSocketReadingWithReconnect),
                    "Error while reading socket connection");

                // This delays the retrying, but only when previous attempt was too fast
                var delaySeconds = Math.Max(0, maxDelaySeconds - sw.Elapsed.TotalSeconds);

                try
                {
                    logger.LogInformation("{where} {what} {delaySeconds}", nameof(RunWebSocketReadingWithReconnect),
                        "Delaying", delaySeconds);
                    await Task.Delay(TimeSpan.FromSeconds(delaySeconds), cancellationToken);
                }
                catch (OperationCanceledException)
                {
                }
            }
        }
    }

    async Task RunOnSocket(string streamUrl, ChannelWriter<Message> channelWriter, CancellationToken cancellationToken)
    {
        using var ws = new ClientWebSocket();
        ws.Options.KeepAliveInterval = TimeSpan.FromSeconds(10);
        ws.Options.KeepAliveTimeout = TimeSpan.FromSeconds(10);
        await ws.ConnectAsync(new Uri(streamUrl), cancellationToken);
        logger.LogInformation("{where} {what} {streamUrl}", nameof(RunOnSocket), "Connected", streamUrl);

        var buffer = new ArrayBufferWriter<byte>();

        while (!cancellationToken.IsCancellationRequested)
        {
            var receive = ws.ReceiveAsync(buffer.GetMemory(512), cancellationToken);
            var result = await receive;

            if (result.MessageType == WebSocketMessageType.Close)
            {
                logger.LogWarning("{where} {what}", nameof(RunOnSocket), "WebSocketMessageType.Close");
                break;
            }

            if (result.MessageType == WebSocketMessageType.Text)
            {
                buffer.Advance(result.Count);
            }

            if (result.EndOfMessage)
            {
                var rented = ArrayPool<byte>.Shared.Rent(buffer.WrittenCount);
                Array.Clear(rented, 0, buffer.WrittenCount);
                var contents = rented.AsMemory(0, buffer.WrittenCount);
                buffer.WrittenMemory.CopyTo(contents);
                var message = new Message(contents, rented);

                channelWriter.TryWrite(message);
                buffer.ResetWrittenCount();
            }
        }
    }

    Dictionary<byte[], string>.AlternateLookup<ReadOnlySpan<byte>> streamNames =
        new Dictionary<byte[], string>(new SpanByteArrayByteAlternateEqualityComparer())
            .GetAlternateLookup<ReadOnlySpan<byte>>();

    public (string stream, ReadOnlyMemory<byte> data) PartiallyParseMessage(ReadOnlyMemory<byte> json)
    {
        var reader = new Utf8JsonReader(json.Span, isFinalBlock: true, default);
        string? stream = "unknown";
        ReadOnlyMemory<byte> data = default;

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.PropertyName)
            {
                if (reader.ValueTextEquals("stream"u8))
                {
                    reader.Read();
                    var streamNameSpan = reader.ValueSpan;
                    if (!streamNames.TryGetValue(streamNameSpan, out stream))
                    {
                        streamNames[streamNameSpan] = stream = reader.GetString()!;
                    }
                }
                else if (reader.ValueTextEquals("data"u8))
                {
                    if (reader.Read() && reader.TokenType == JsonTokenType.StartObject)
                    {
                        int startIndex = (int)reader.TokenStartIndex;

                        if (!reader.TrySkip() || !reader.Read())
                        {
                            throw new JsonException("Failed to parse inside of the data");
                        }

                        int endIndex = (int)reader.TokenStartIndex + 1;
                        data = json[startIndex..endIndex];
                    }
                }
            }
        }

        if (stream is null)
        {
            throw new JsonException("Required stream property is missing from json data");
        }

        return (stream, data);
    }
}

public class SpanByteArrayByteAlternateEqualityComparer :
    IEqualityComparer<byte[]>,
    IAlternateEqualityComparer<ReadOnlySpan<byte>, byte[]>
{
    public byte[] Create(ReadOnlySpan<byte> alternate)
    {
        return alternate.ToArray();
    }

    public bool Equals(ReadOnlySpan<byte> x, byte[] y)
    {
        return x.SequenceEqual(y);
    }

    public bool Equals(byte[]? x, byte[]? y)
    {
        return x.AsSpan().SequenceEqual(y);
    }

    public int GetHashCode(ReadOnlySpan<byte> obj)
    {
        var hashCode = new HashCode();
        hashCode.AddBytes(obj);
        return hashCode.ToHashCode();
    }

    public int GetHashCode([DisallowNull] byte[] obj)
    {
        return GetHashCode(obj.AsSpan());
    }
}

record struct Message(ReadOnlyMemory<byte> Contents, byte[] Buffer);