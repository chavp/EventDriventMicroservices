namespace Mti.Domain.Messaging.SavePartiesByOrder
{
    public sealed record VehicleAssetRequest()
    {
        public string? Brand { get; set; }
        public string? Model { get; set; }
        public string? Color { get; set; }
        public string? RegisterNo { get; set; }
        public string? RegisterProvince { get; set; }
        public ushort? RegisterYear { get; set; }
        public string? Chassis { get; set; }
        public float? Cc { get; set; }
        public float? Seat { get; set; }
        public float? Weight { get; set; }
        public float? Tonnage { get; set; }
        public string? Engine { get; set; }
        public ushort? Passenger { get; set; }
    }
}
