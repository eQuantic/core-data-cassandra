using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Cassandra.Data.Linq;
using eQuantic.Core.Data.Repository;
using eQuantic.Core.Data.Repository.Read;
using eQuantic.Core.Linq;
using eQuantic.Core.Linq.Extensions;
using eQuantic.Core.Linq.Specification;

namespace eQuantic.Core.Data.Cassandra.Repository.Read
{
    public class AsyncReadRepository<TUnitOfWork, TEntity, TKey> : ReadRepository<TUnitOfWork, TEntity, TKey>, IAsyncReadRepository<TUnitOfWork, TEntity, TKey>
        where TUnitOfWork : IQueryableUnitOfWork
        where TEntity : class, IEntity, new()
    {
        public AsyncReadRepository(TUnitOfWork unitOfWork) : base(unitOfWork)
        {
        }

        public async Task<IEnumerable<TEntity>> AllMatchingAsync(ISpecification<TEntity> specification)
        {
            return await GetSet().Where(specification.SatisfiedBy()).ExecuteAsync();
        }

        public async Task<long> CountAsync()
        {
            return await GetSet().Count().ExecuteAsync();
        }

        public async Task<long> CountAsync(ISpecification<TEntity> specification)
        {
            return await GetSet().Where(specification.SatisfiedBy()).Count().ExecuteAsync();
        }

        public async Task<long> CountAsync(Expression<Func<TEntity, bool>> filter)
        {
            return await GetSet().Where(filter).Count().ExecuteAsync();
        }

        public async Task<IEnumerable<TEntity>> GetAllAsync()
        {
            return await GetSet().ExecuteAsync();
        }

        public async Task<IEnumerable<TEntity>> GetAllAsync(ISorting[] sortingColumns)
        {
            return await ((CqlQuery<TEntity>)GetSet().OrderBy(sortingColumns)).ExecuteAsync();
        }

        public async Task<TEntity> GetAsync(TKey id)
        {
            return id != null ? await GetSet().Find(id).ExecuteAsync() : null;
        }

        public async Task<IEnumerable<TEntity>> GetFilteredAsync(Expression<Func<TEntity, bool>> filter)
        {
            return await GetSet().Where(filter).ExecuteAsync();
        }

        public async Task<IEnumerable<TEntity>> GetFilteredAsync(Expression<Func<TEntity, bool>> filter, ISorting[] sortColumns)
        {
            return await ((CqlQuery<TEntity>)GetSet().Where(filter).OrderBy(sortColumns)).ExecuteAsync();
        }

        public async Task<TEntity> GetFirstAsync(Expression<Func<TEntity, bool>> filter)
        {
            return await GetSet().FirstOrDefault(filter).ExecuteAsync();
        }

        public async Task<IEnumerable<TEntity>> GetPagedAsync(int limit, ISorting[] sortColumns)
        {
            return await GetPagedAsync((Expression<Func<TEntity, bool>>)null, 1, limit, sortColumns);
        }

        public async Task<IEnumerable<TEntity>> GetPagedAsync(ISpecification<TEntity> specification, int limit, ISorting[] sortColumns)
        {
            return await GetPagedAsync(specification.SatisfiedBy(), 1, limit, sortColumns);
        }

        public async Task<IEnumerable<TEntity>> GetPagedAsync(Expression<Func<TEntity, bool>> filter, int limit, ISorting[] sortColumns)
        {
            return await GetPagedAsync(filter, 1, limit, sortColumns);
        }

        public async Task<IEnumerable<TEntity>> GetPagedAsync(int pageIndex, int pageCount, ISorting[] sortColumns)
        {
            return await GetPagedAsync((Expression<Func<TEntity, bool>>)null, pageIndex, pageCount, sortColumns);
        }

        public async Task<IEnumerable<TEntity>> GetPagedAsync(ISpecification<TEntity> specification, int pageIndex, int pageCount, ISorting[] sortColumns)
        {
            return await GetPagedAsync(specification.SatisfiedBy(), pageIndex, pageCount, sortColumns);
        }

        public async Task<IEnumerable<TEntity>> GetPagedAsync(Expression<Func<TEntity, bool>> filter, int pageIndex, int pageCount, ISorting[] sortColumns)
        {
            CqlQuery<TEntity> query = GetSet();
            if (filter != null) query = query.Where(filter);

            if (sortColumns != null && sortColumns.Length > 0)
            {
                query = (CqlQuery<TEntity>)query.OrderBy(sortColumns);
            }
            if (pageCount > 0)
            {
                int skip = (pageIndex - 1) * pageCount;
                var pagingState = (await query.SetPageSize(skip).ExecutePagedAsync()).CurrentPagingState;
                return await query.SetPagingState(pagingState).SetPageSize(pageCount).ExecutePagedAsync();
            }

            return await query.ExecuteAsync();
        }

        public Task<TEntity> GetSingleAsync(Expression<Func<TEntity, bool>> filter)
        {
            return GetSet().FirstOrDefault(filter).ExecuteAsync();
        }

        public Task<TEntity> GetSingleAsync(Expression<Func<TEntity, bool>> filter, ISorting[] sortingColumns)
        {
            CqlQuery<TEntity> query = GetSet();
            if (sortingColumns != null && sortingColumns.Length > 0)
            {
                query = (CqlQuery<TEntity>)query.OrderBy(sortingColumns);
            }
            return query.Where(filter).FirstOrDefault().ExecuteAsync();
        }
    }
}