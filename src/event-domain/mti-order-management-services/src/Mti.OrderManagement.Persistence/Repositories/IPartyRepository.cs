namespace Mti.OrderManagement.Persistence.Repositories
{
    public interface IPartyRepository
    {
        IReadOnlyCollection<string> GetOrganizationTitles();
    }
}
