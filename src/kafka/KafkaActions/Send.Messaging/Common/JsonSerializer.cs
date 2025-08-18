using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Newtonsoft.Json;
using Send.Messaging.Send;

namespace Send.Messaging.Common
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
