using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mti.OrderManagement.Infrastructure.Streaming.Settings
{
    public sealed class StreamBrokerSettings
    {
        public const string SettingsKey = "StreamBroker";

        public string BootstrapServers { get; set; }
        public string? GroupId { get; set; }
        public string? ClientId { get; set; }

        public string Topic { get; set; }
    }
}
