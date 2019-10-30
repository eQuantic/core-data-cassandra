using System;
using System.Linq;
using System.Linq.Expressions;
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

        public Task AddAsync(TEntity item)
        {
            throw new NotImplementedException();
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
            throw new NotImplementedException();
        }

        public Task ModifyAsync(TEntity item)
        {
            throw new NotImplementedException();
        }

        public Task RemoveAsync(TEntity item)
        {
            throw new NotImplementedException();
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