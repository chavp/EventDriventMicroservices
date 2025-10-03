using System.Text;
using Confluent.Kafka;
using Newtonsoft.Json;

namespace Mti.ProductManagement.Services.Api.Infrastructure
{
    public class JsonSerializer<T> : ISerializer<T>
    {
        public byte[] Serialize(T data, SerializationContext context)
        {
            if (data == null) return [];
            // Implement your serialization logic here (e.g., JSON serialization)
            var jsonData = JsonConvert.SerializeObject(data);
            return Encoding.UTF8.GetBytes(jsonData);
        }
    }
}
