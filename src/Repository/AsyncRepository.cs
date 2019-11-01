using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Cassandra.Data.Linq;
using eQuantic.Core.Data.Repository;
using eQuantic.Core.Data.Repository.Sql;
using eQuantic.Core.Linq;
using eQuantic.Core.Linq.Extensions;
using eQuantic.Core.Linq.Specification;

namespace eQuantic.Core.Data.Cassandra.Repository
{
    public class AsyncRepository<TUnitOfWork, TEntity, TKey> : Repository<TUnitOfWork, TEntity, TKey>, IAsyncRepository<TUnitOfWork, TEntity, TKey>
        where TUnitOfWork : IQueryableUnitOfWork
        where TEntity : class, IEntity, new()
    {
        public AsyncRepository(TUnitOfWork unitOfWork) : base(unitOfWork)
        {
        }

        public async Task AddAsync(TEntity item)
        {
            if (item == null) return;
            await GetSet().Insert(item).ExecuteAsync();
        }

        public async Task<IEnumerable<TEntity>> AllMatchingAsync(ISpecification<TEntity> specification)
        {
            if (specification == null)
                throw new ArgumentException("Specification cannot be null", nameof(specification));

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

        /// <summary>
        /// <see cref="eQuantic.Core.Data.Repository.IRepository{TUnitOfWork, TEntity, TKey}"/>
        /// </summary>
        /// <param name="filter">
        /// <see cref="eQuantic.Core.Data.Repository.IRepository{TUnitOfWork, TEntity, TKey}"/>
        /// </param>
        /// <returns></returns>
        public async Task<int> DeleteManyAsync(Expression<Func<TEntity, bool>> filter)
        {
            var rowSet = await GetSet().Where(filter).Delete().ExecuteAsync();
            return rowSet.Count();
        }

        /// <summary>
        /// <see cref="eQuantic.Core.Data.Repository.IRepository{TUnitOfWork, TEntity, TKey}"/>
        /// </summary>
        /// <param name="specification">
        /// <see cref="eQuantic.Core.Data.Repository.IRepository{TUnitOfWork, TEntity, TKey}"/>
        /// </param>
        /// <returns></returns>
        public async Task<int> DeleteManyAsync(ISpecification<TEntity> specification)
        {
            return await DeleteManyAsync(specification.SatisfiedBy());
        }

        public async Task<IEnumerable<TEntity>> GetAllAsync()
        {
            return await GetSet().ExecuteAsync();
        }

        public async Task<IEnumerable<TEntity>> GetAllAsync(ISorting[] sortingColumns)
        {
            CqlQuery<TEntity> query = GetSet();

            if (sortingColumns != null && sortingColumns.Length > 0)
            {
                query = (CqlQuery<TEntity>)query.OrderBy(sortingColumns);
            }
            return await query.ExecuteAsync();
        }

        public async Task<TEntity> GetAsync(TKey id)
        {
            if (id == null) throw new ArgumentException("Id cannot be null", nameof(id));
            return await GetSet().Find(id).ExecuteAsync();
        }

        public async Task<IEnumerable<TEntity>> GetFilteredAsync(Expression<Func<TEntity, bool>> filter)
        {
            if (filter == null)
                throw new ArgumentException("Filter expression cannot be null", nameof(filter));

            return await GetSet().Where(filter).ExecuteAsync();
        }

        public async Task<IEnumerable<TEntity>> GetFilteredAsync(Expression<Func<TEntity, bool>> filter, ISorting[] sortColumns)
        {
            if (filter == null)
                throw new ArgumentException("Filter expression cannot be null", nameof(filter));

            var query = GetSet().Where(filter);
            if (sortColumns != null && sortColumns.Length > 0)
            {
                query = (CqlQuery<TEntity>)query.OrderBy(sortColumns);
            }
            return await query.ExecuteAsync();
        }

        public async Task<TEntity> GetFirstAsync(Expression<Func<TEntity, bool>> filter)
        {
            return await GetSet().Where(filter).FirstOrDefault().ExecuteAsync();
        }

        public async Task<IEnumerable<TEntity>> GetPagedAsync(int limit, ISorting[] sortColumns)
        {
            return await GetPagedAsync(1, limit, sortColumns);
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
            var query = filter != null ? GetSet().Where(filter) : GetSet();

            if (sortColumns != null && sortColumns.Length > 0)
            {
                query = (CqlQuery<TEntity>)query.OrderBy(sortColumns);
            }
            if (pageCount > 0)
                return query.Skip((pageIndex - 1) * pageCount).Take(pageCount);

            return await query.ExecuteAsync();
        }

        public async Task<TEntity> GetSingleAsync(Expression<Func<TEntity, bool>> filter)
        {
            return await GetSet().Where(filter).FirstOrDefault().ExecuteAsync();
        }

        public async Task<TEntity> GetSingleAsync(Expression<Func<TEntity, bool>> filter, ISorting[] sortingColumns)
        {
            return await ((CqlQuery<TEntity>)GetSet().OrderBy(sortingColumns)).Where(filter).FirstOrDefault().ExecuteAsync();
        }

        public Task MergeAsync(TEntity persisted, TEntity current)
        {
            throw new NotImplementedException();
        }

        public Task ModifyAsync(TEntity item)
        {
            throw new NotImplementedException();
        }

        public async Task RemoveAsync(TEntity item)
        {
            if (item == (TEntity)null) return;

            UnitOfWork.Attach(item);

            var key = GetSet().GetKeyValue<TKey>(item);
            var expression = GetSet().GetKeyExpression(key);
            await GetSet().Where(expression).Delete().ExecuteAsync();
        }

        /// <summary>
        /// <see cref="eQuantic.Core.Data.Repository.IRepository{TUnitOfWork, TEntity, TKey}"/>
        /// </summary>
        /// <param name="filter">
        /// <see cref="eQuantic.Core.Data.Repository.IRepository{TUnitOfWork, TEntity, TKey}"/>
        /// </param>
        /// <param name="updateFactory">
        /// <see cref="eQuantic.Core.Data.Repository.IRepository{TUnitOfWork, TEntity, TKey}"/>
        /// </param>
        /// <returns></returns>
        public async Task<int> UpdateManyAsync(Expression<Func<TEntity, bool>> filter, Expression<Func<TEntity, TEntity>> updateFactory)
        {
            var rowSet = await GetSet().Where(filter).Select(updateFactory).Update().ExecuteAsync();
            return rowSet.Count();
        }

        /// <summary>
        /// <see cref="eQuantic.Core.Data.Repository.IRepository{TUnitOfWork, TEntity, TKey}"/>
        /// </summary>
        /// <param name="specification">
        /// <see cref="eQuantic.Core.Data.Repository.IRepository{TUnitOfWork, TEntity, TKey}"/>
        /// </param>
        /// <param name="updateFactory">
        /// <see cref="eQuantic.Core.Data.Repository.IRepository{TUnitOfWork, TEntity, TKey}"/>
        /// </param>
        /// <returns></returns>
        public async Task<int> UpdateManyAsync(ISpecification<TEntity> specification, Expression<Func<TEntity, TEntity>> updateFactory)
        {
            return await UpdateManyAsync(specification.SatisfiedBy(), updateFactory);
        }
    }
}