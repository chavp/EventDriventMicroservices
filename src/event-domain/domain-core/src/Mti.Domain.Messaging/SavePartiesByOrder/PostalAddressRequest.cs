namespace Mti.Domain.Messaging.SavePartiesByOrder
{
    public sealed record PostalAddressRequest()
    {
        public string? Name { get; init; }
        public string? HouseNumber { get; init; }
        public string? VillageNumber { get; init; }
        public string? Village { get; init; }
        public string? Alley { get; init; }
        public string? Road { get; init; }
        public string? Building { get; init; }
        public string? Room { get; init; }
        public string? Floor { get; init; }
        public string? Province { get; init; }
        public string? District { get; init; }
        public string? SubDistrict { get; init; }
        public string? ZipCode { get; init; }
    }
}
