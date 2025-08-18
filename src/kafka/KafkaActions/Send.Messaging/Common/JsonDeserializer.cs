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
    public class JsonDeserializer<T>
        : IDeserializer<T>
    {
        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull) return default(T);
            // Implement your deserialization logic here (e.g., JSON deserialization)
            var jsonString = Encoding.UTF8.GetString(data.ToArray());
            return JsonConvert.DeserializeObject<T>(jsonString);
        }
    }
}
