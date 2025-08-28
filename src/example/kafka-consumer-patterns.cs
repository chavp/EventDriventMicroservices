using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Confluent.Kafka;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Linq;

// โมเดลสำหรับข้อมูลที่จะส่ง
public class OrderMessage
{
    public string OrderId { get; set; }
    public string ProductName { get; set; }
    public int Quantity { get; set; }
    public decimal Price { get; set; }
    public DateTime OrderDate { get; set; }
}

public class MessageWithMetadata
{
    public ConsumeResult<string, string> ConsumeResult { get; set; }
    public OrderMessage Order { get; set; }
    public DateTime ReceivedAt { get; set; }
}

// แบบที่ 1: Sequential Processing (แบบเดิม)
public class SequentialKafkaConsumer
{
    private readonly IConsumer<string, string> _consumer;
    
    public SequentialKafkaConsumer(string bootstrapServers, string groupId)
    {
        var config = new ConsumerConfig
        {
            GroupId = groupId,
            BootstrapServers = bootstrapServers,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };
        
        _consumer = new ConsumerBuilder<string, string>(config).Build();
    }
    
    public async Task StartConsumingAsync(string topic, CancellationToken cancellationToken)
    {
        _consumer.Subscribe(topic);
        Console.WriteLine("🔄 Sequential Consumer Started");
        
        while (!cancellationToken.IsCancellationRequested)
        {
            var consumeResult = _consumer.Consume(cancellationToken);
            
            // Process แบบ Sequential - รอให้จบก่อนถึงจะไป consume ต่อ
            await ProcessMessageSequentialAsync(consumeResult);
            
            _consumer.Commit(consumeResult);
        }
    }
    
    private async Task ProcessMessageSequentialAsync(ConsumeResult<string, string> result)
    {
        var order = JsonConvert.DeserializeObject<OrderMessage>(result.Value);
        Console.WriteLine($"📦 [Sequential] Processing: {order.OrderId}");
        
        // จำลองการทำงานที่ใช้เวลานาน
        await Task.Delay(3000);
        
        Console.WriteLine($"✅ [Sequential] Completed: {order.OrderId}");
    }
}

// แบบที่ 2: Fire-and-Forget Parallel Processing
public class FireAndForgetKafkaConsumer
{
    private readonly IConsumer<string, string> _consumer;
    
    public FireAndForgetKafkaConsumer(string bootstrapServers, string groupId)
    {
        var config = new ConsumerConfig
        {
            GroupId = groupId,
            BootstrapServers = bootstrapServers,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };
        
        _consumer = new ConsumerBuilder<string, string>(config).Build();
    }
    
    public async Task StartConsumingAsync(string topic, CancellationToken cancellationToken)
    {
        _consumer.Subscribe(topic);
        Console.WriteLine("🚀 Fire-and-Forget Consumer Started");
        
        while (!cancellationToken.IsCancellationRequested)
        {
            var consumeResult = _consumer.Consume(cancellationToken);
            
            // ส่งไป process แบบ parallel ทันที ไม่รอผลลัพธ์
            _ = Task.Run(async () => await ProcessMessageAsync(consumeResult));
            
            // Commit ทันที (อันตราย! อาจทำให้ message หายได้)
            _consumer.Commit(consumeResult);
        }
    }
    
    private async Task ProcessMessageAsync(ConsumeResult<string, string> result)
    {
        var order = JsonConvert.DeserializeObject<OrderMessage>(result.Value);
        Console.WriteLine($"📦 [Fire&Forget] Processing: {order.OrderId}");
        
        await Task.Delay(3000);
        
        Console.WriteLine($"✅ [Fire&Forget] Completed: {order.OrderId}");
    }
}

// แบบที่ 3: Producer-Consumer Pattern with Channel
public class ProducerConsumerKafkaProcessor
{
    private readonly IConsumer<string, string> _consumer;
    private readonly Channel<MessageWithMetadata> _processingQueue;
    private readonly int _maxParallelTasks;
    
    public ProducerConsumerKafkaProcessor(string bootstrapServers, string groupId, int maxParallelTasks = 5)
    {
        var config = new ConsumerConfig
        {
            GroupId = groupId,
            BootstrapServers = bootstrapServers,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            MaxPollIntervalMs = 300000 // 5 นาที
        };
        
        _consumer = new ConsumerBuilder<string, string>(config).Build();
        _maxParallelTasks = maxParallelTasks;
        
        // สร้าง channel สำหรับ queue ข้อมูล
        var options = new BoundedChannelOptions(100) // queue ได้สูงสุด 100 messages
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = true
        };
        _processingQueue = Channel.CreateBounded<MessageWithMetadata>(options);
    }
    
    public async Task StartConsumingAsync(string topic, CancellationToken cancellationToken)
    {
        _consumer.Subscribe(topic);
        Console.WriteLine($"⚡ Producer-Consumer Started (Max Parallel: {_maxParallelTasks})");
        
        // เริ่ม worker tasks สำหรับ process ข้อมูล
        var processingTasks = Enumerable.Range(0, _maxParallelTasks)
            .Select(i => ProcessWorkerAsync(i, cancellationToken))
            .ToArray();
        
        // Consumer loop
        var consumerTask = Task.Run(async () =>
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var consumeResult = _consumer.Consume(cancellationToken);
                    
                    var order = JsonConvert.DeserializeObject<OrderMessage>(consumeResult.Value);
                    var messageWithMetadata = new MessageWithMetadata
                    {
                        ConsumeResult = consumeResult,
                        Order = order,
                        ReceivedAt = DateTime.UtcNow
                    };
                    
                    // ส่งเข้า queue เพื่อให้ worker process
                    await _processingQueue.Writer.WriteAsync(messageWithMetadata, cancellationToken);
                }
            }
            finally
            {
                _processingQueue.Writer.Complete();
            }
        });
        
        // รอให้ทุก task เสร็จ
        await Task.WhenAll(new[] { consumerTask }.Concat(processingTasks));
    }
    
    private async Task ProcessWorkerAsync(int workerId, CancellationToken cancellationToken)
    {
        Console.WriteLine($"👷 Worker {workerId} started");
        
        await foreach (var message in _processingQueue.Reader.ReadAllAsync(cancellationToken))
        {
            try
            {
                Console.WriteLine($"📦 [Worker-{workerId}] Processing: {message.Order.OrderId}");
                
                // จำลองการทำงาน
                await Task.Delay(3000, cancellationToken);
                
                Console.WriteLine($"✅ [Worker-{workerId}] Completed: {message.Order.OrderId}");
                
                // Commit หลังจาก process เสร็จ
                _consumer.Commit(message.ConsumeResult);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ [Worker-{workerId}] Error processing {message.Order.OrderId}: {ex.Message}");
                // ในกรณีจริง อาจต้องส่งไป dead letter queue
            }
        }
        
        Console.WriteLine($"👷 Worker {workerId} finished");
    }
}

// แบบที่ 4: Batch Processing with Parallel Execution
public class BatchParallelKafkaConsumer
{
    private readonly IConsumer<string, string> _consumer;
    private readonly int _batchSize;
    private readonly int _maxParallelTasks;
    
    public BatchParallelKafkaConsumer(string bootstrapServers, string groupId, int batchSize = 10, int maxParallelTasks = 3)
    {
        var config = new ConsumerConfig
        {
            GroupId = groupId,
            BootstrapServers = bootstrapServers,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };
        
        _consumer = new ConsumerBuilder<string, string>(config).Build();
        _batchSize = batchSize;
        _maxParallelTasks = maxParallelTasks;
    }
    
    public async Task StartConsumingAsync(string topic, CancellationToken cancellationToken)
    {
        _consumer.Subscribe(topic);
        Console.WriteLine($"📦 Batch Parallel Consumer Started (Batch: {_batchSize}, Parallel: {_maxParallelTasks})");
        
        var batch = new List<ConsumeResult<string, string>>();
        
        while (!cancellationToken.IsCancellationRequested)
        {
            var consumeResult = _consumer.Consume(TimeSpan.FromSeconds(1));
            if (consumeResult != null)
            {
                batch.Add(consumeResult);
            }
            
            // Process batch เมื่อครบ size หรือ timeout
            if (batch.Count >= _batchSize || (batch.Count > 0 && consumeResult == null))
            {
                await ProcessBatchAsync(batch.ToList());
                
                // Commit ทั้ง batch
                foreach (var result in batch)
                {
                    _consumer.Commit(result);
                }
                
                batch.Clear();
            }
        }
        
        // Process batch ที่เหลือ
        if (batch.Count > 0)
        {
            await ProcessBatchAsync(batch);
            foreach (var result in batch)
            {
                _consumer.Commit(result);
            }
        }
    }
    
    private async Task ProcessBatchAsync(List<ConsumeResult<string, string>> batch)
    {
        Console.WriteLine($"🔄 Processing batch of {batch.Count} messages");
        
        // แบ่งเป็น chunks เพื่อ process parallel
        var chunks = batch
            .Select((item, index) => new { item, index })
            .GroupBy(x => x.index % _maxParallelTasks)
            .Select(g => g.Select(x => x.item).ToList())
            .ToList();
        
        var tasks = chunks.Select(async chunk =>
        {
            foreach (var result in chunk)
            {
                var order = JsonConvert.DeserializeObject<OrderMessage>(result.Value);
                Console.WriteLine($"📦 [Batch] Processing: {order.OrderId}");
                
                await Task.Delay(2000); // จำลองการทำงาน
                
                Console.WriteLine($"✅ [Batch] Completed: {order.OrderId}");
            }
        });
        
        await Task.WhenAll(tasks);
        Console.WriteLine($"✨ Batch processing completed!");
    }
}

// แบบที่ 5: Multi-Partition Parallel Consumer
public class MultiPartitionKafkaConsumer
{
    private readonly string _bootstrapServers;
    private readonly string _groupId;
    private readonly string _topic;
    
    public MultiPartitionKafkaConsumer(string bootstrapServers, string groupId, string topic)
    {
        _bootstrapServers = bootstrapServers;
        _groupId = groupId;
        _topic = topic;
    }
    
    public async Task StartMultipleConsumersAsync(int consumerCount, CancellationToken cancellationToken)
    {
        Console.WriteLine($"🚀 Starting {consumerCount} parallel consumers");
        
        var tasks = Enumerable.Range(0, consumerCount)
            .Select(i => StartSingleConsumerAsync(i, cancellationToken))
            .ToArray();
        
        await Task.WhenAll(tasks);
    }
    
    private async Task StartSingleConsumerAsync(int consumerId, CancellationToken cancellationToken)
    {
        var config = new ConsumerConfig
        {
            GroupId = _groupId,
            BootstrapServers = _bootstrapServers,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };
        
        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(_topic);
        
        Console.WriteLine($"👤 Consumer {consumerId} started");
        
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var consumeResult = consumer.Consume(cancellationToken);
                
                var order = JsonConvert.DeserializeObject<OrderMessage>(consumeResult.Value);
                Console.WriteLine($"📦 [Consumer-{consumerId}] Processing: {order.OrderId} from partition {consumeResult.Partition}");
                
                await Task.Delay(2000, cancellationToken);
                
                Console.WriteLine($"✅ [Consumer-{consumerId}] Completed: {order.OrderId}");
                
                consumer.Commit(consumeResult);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ [Consumer-{consumerId}] Error: {ex.Message}");
            }
        }
        
        Console.WriteLine($"👤 Consumer {consumerId} stopped");
    }
}

// ตัวอย่างการใช้งาน
class Program
{
    private const string BOOTSTRAP_SERVERS = "localhost:9092";
    private const string TOPIC_NAME = "orders";
    private const string CONSUMER_GROUP = "parallel-processors";

    static async Task Main(string[] args)
    {
        Console.WriteLine("🚀 Kafka Parallel Processing Examples");
        Console.WriteLine("Choose processing pattern:");
        Console.WriteLine("1. Sequential Processing (แบบเดิม)");
        Console.WriteLine("2. Fire-and-Forget Parallel");
        Console.WriteLine("3. Producer-Consumer Pattern");
        Console.WriteLine("4. Batch Parallel Processing");
        Console.WriteLine("5. Multi-Partition Consumers");
        Console.WriteLine("6. Performance Comparison");
        
        var choice = Console.ReadLine();
        var cancellationTokenSource = new CancellationTokenSource();
        
        // Handle Ctrl+C
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cancellationTokenSource.Cancel();
            Console.WriteLine("\n🛑 Shutting down...");
        };
        
        try
        {
            switch (choice)
            {
                case "1":
                    await RunSequentialConsumer(cancellationTokenSource.Token);
                    break;
                case "2":
                    await RunFireAndForgetConsumer(cancellationTokenSource.Token);
                    break;
                case "3":
                    await RunProducerConsumerPattern(cancellationTokenSource.Token);
                    break;
                case "4":
                    await RunBatchParallelConsumer(cancellationTokenSource.Token);
                    break;
                case "5":
                    await RunMultiPartitionConsumers(cancellationTokenSource.Token);
                    break;
                case "6":
                    await RunPerformanceComparison(cancellationTokenSource.Token);
                    break;
                default:
                    Console.WriteLine("Invalid option!");
                    break;
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("Operation cancelled by user.");
        }
    }
    
    static async Task RunSequentialConsumer(CancellationToken cancellationToken)
    {
        var consumer = new SequentialKafkaConsumer(BOOTSTRAP_SERVERS, CONSUMER_GROUP + "-sequential");
        await consumer.StartConsumingAsync(TOPIC_NAME, cancellationToken);
    }
    
    static async Task RunFireAndForgetConsumer(CancellationToken cancellationToken)
    {
        var consumer = new FireAndForgetKafkaConsumer(BOOTSTRAP_SERVERS, CONSUMER_GROUP + "-fireforget");
        await consumer.StartConsumingAsync(TOPIC_NAME, cancellationToken);
    }
    
    static async Task RunProducerConsumerPattern(CancellationToken cancellationToken)
    {
        var consumer = new ProducerConsumerKafkaProcessor(BOOTSTRAP_SERVERS, CONSUMER_GROUP + "-prodcons", 3);
        await consumer.StartConsumingAsync(TOPIC_NAME, cancellationToken);
    }
    
    static async Task RunBatchParallelConsumer(CancellationToken cancellationToken)
    {
        var consumer = new BatchParallelKafkaConsumer(BOOTSTRAP_SERVERS, CONSUMER_GROUP + "-batch", 5, 3);
        await consumer.StartConsumingAsync(TOPIC_NAME, cancellationToken);
    }
    
    static async Task RunMultiPartitionConsumers(CancellationToken cancellationToken)
    {
        var consumer = new MultiPartitionKafkaConsumer(BOOTSTRAP_SERVERS, CONSUMER_GROUP + "-multi", TOPIC_NAME);
        await consumer.StartMultipleConsumersAsync(3, cancellationToken); // 3 consumers
    }
    
    static async Task RunPerformanceComparison(CancellationToken cancellationToken)
    {
        Console.WriteLine("📊 Performance Comparison");
        Console.WriteLine("Running different patterns for 30 seconds each...\n");
        
        var patterns = new[]
        {
            ("Sequential", () => new SequentialKafkaConsumer(BOOTSTRAP_SERVERS, CONSUMER_GROUP + "-perf-seq")),
            ("Producer-Consumer", () => new ProducerConsumerKafkaProcessor(BOOTSTRAP_SERVERS, CONSUMER_GROUP + "-perf-pc", 3))
        };
        
        foreach (var (name, factory) in patterns)
        {
            Console.WriteLine($"🔄 Testing {name} pattern...");
            var testCancellation = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            
            try
            {
                if (factory() is SequentialKafkaConsumer seqConsumer)
                {
                    await seqConsumer.StartConsumingAsync(TOPIC_NAME, testCancellation.Token);
                }
                else if (factory() is ProducerConsumerKafkaProcessor pcConsumer)
                {
                    await pcConsumer.StartConsumingAsync(TOPIC_NAME, testCancellation.Token);
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine($"✅ {name} test completed\n");
            }
        }
    }
}