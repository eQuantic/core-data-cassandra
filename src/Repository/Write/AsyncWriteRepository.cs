using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using Cassandra.Data.Linq;
using eQuantic.Core.Data.Repository;
using eQuantic.Core.Data.Repository.Sql;
using eQuantic.Core.Data.Repository.Write;
using eQuantic.Core.Linq.Specification;

namespace eQuantic.Core.Data.Cassandra.Repository.Write
{
    public class AsyncWriteRepository<TUnitOfWork, TEntity, TKey> : WriteRepository<TUnitOfWork, TEntity, TKey>, IAsyncWriteRepository<TUnitOfWork, TEntity, TKey>
        where TUnitOfWork : IQueryableUnitOfWork
        where TEntity : class, IEntity, new()
    {
        public AsyncWriteRepository(TUnitOfWork unitOfWork) : base(unitOfWork)
        {
        }

        public async Task AddAsync(TEntity item)
        {
            if (item == null) return;
            await GetSet().Insert(item).ExecuteAsync();
        }

        public async Task<int> DeleteManyAsync(Expression<Func<TEntity, bool>> filter)
        {
            var rowSet = await GetSet().Where(filter).Delete().ExecuteAsync();
            return rowSet.Count();
        }

        public async Task<int> DeleteManyAsync(ISpecification<TEntity> specification)
        {
            return await DeleteManyAsync(specification.SatisfiedBy());
        }

        public Task MergeAsync(TEntity persisted, TEntity current)
        {
            var entityType = typeof(TEntity);

#if NETSTANDARD1_6 || NETSTANDARD2_0
            var properties = entityType.GetTypeInfo().GetProperties().Where(prop => prop.CanRead && prop.CanWrite);
#else
            var properties = entityType.GetProperties().Where(prop => prop.CanRead && prop.CanWrite);
#endif

            foreach (var prop in properties)
            {
                var value = prop.GetValue(current, null);
                if (value != null)
                    prop.SetValue(persisted, value, null);
            }

            return null;
        }

        public Task ModifyAsync(TEntity item)
        {
            throw new NotImplementedException();
        }

        public async Task RemoveAsync(TEntity item)
        {
            if (item == (TEntity)null) return;

            //attach item if not exist
            UnitOfWork.Attach(item);

            var key = GetSet().GetKeyValue<TKey>(item);
            var expression = GetSet().GetKeyExpression(key);
            await GetSet().Where(expression).Delete().ExecuteAsync();
        }

        public async Task<int> UpdateManyAsync(Expression<Func<TEntity, bool>> filter, Expression<Func<TEntity, TEntity>> updateFactory)
        {
            var rowSet = await GetSet().Where(filter).Select(updateFactory).ExecuteAsync();
            return rowSet.Count();
        }

        public async Task<int> UpdateManyAsync(ISpecification<TEntity> specification, Expression<Func<TEntity, TEntity>> updateFactory)
        {
            return await UpdateManyAsync(specification.SatisfiedBy(), updateFactory);
        }
    }
}