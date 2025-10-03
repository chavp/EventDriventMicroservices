using System.Text;
using Confluent.Kafka;
using Newtonsoft.Json;

namespace Mti.ProductManagement.Services.Api.Infrastructure
{
    public sealed class JsonDeserializer<T> : IDeserializer<T?>
    {
        public T? Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull) return default(T);
            // Implement your deserialization logic here (e.g., JSON deserialization)
            var jsonString = Encoding.UTF8.GetString(data.ToArray());
            return JsonConvert.DeserializeObject<T?>(jsonString);
        }
    }
}
