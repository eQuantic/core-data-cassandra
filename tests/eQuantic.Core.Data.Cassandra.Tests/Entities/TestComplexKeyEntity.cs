using eQuantic.Core.Data.Repository;

namespace eQuantic.Core.Data.Cassandra.Tests.Entities
{
    public class TestComplexKeyEntity : IEntity
    {
        public int Test1 { get; set; }
        public int Test2 { get; set; }
    }
}