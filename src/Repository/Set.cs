using System;
using System.Linq;
using Cassandra;
using Cassandra.Data.Linq;
using Cassandra.Mapping;
using eQuantic.Core.Data.Repository;

namespace eQuantic.Core.Data.EntityFramework.Repository
{
    public class Set<TEntity> : Table<TEntity>, Data.Repository.ISet<TEntity> where TEntity : class, IEntity, new()
    {
        public Set(ISession session) : base(session)
        {
        }

        public TEntity Find<TKey>(TKey id)
        {
            var mapping = MappingConfiguration.Global.Get<TEntity>();
            if (mapping?.PartitionKeys.Any() == true)
            {
            }
            return null;
        }
    }
}