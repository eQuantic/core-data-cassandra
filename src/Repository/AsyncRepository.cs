using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
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

        public Task AddAsync(TEntity item)
        {
            throw new NotImplementedException();
        }

        public async Task<IEnumerable<TEntity>> AllMatchingAsync(ISpecification<TEntity> specification)
        {
            return await AllMatchingAsync(specification, new Expression<Func<TEntity, object>>[0]);
        }

        public async Task<IEnumerable<TEntity>> AllMatchingAsync(ISpecification<TEntity> specification, params string[] loadProperties)
        {
            if (specification == null)
                throw new ArgumentException("Specification cannot be null", nameof(specification));

            return await GetQueryable(loadProperties).Where(specification.SatisfiedBy()).ExecuteAsync();
        }

        public async Task<IEnumerable<TEntity>> AllMatchingAsync(ISpecification<TEntity> specification, params Expression<Func<TEntity, object>>[] loadProperties)
        {
            if (specification == null)
                throw new ArgumentException("Specification cannot be null", nameof(specification));

            return await GetQueryable(loadProperties).Where(specification.SatisfiedBy()).ExecuteAsync();
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
            return await GetQueryable(new Expression<Func<TEntity, object>>[0]).ExecuteAsync();
        }

        public async Task<IEnumerable<TEntity>> GetAllAsync(params string[] loadProperties)
        {
            return await GetQueryable(loadProperties).ExecuteAsync();
        }

        public async Task<IEnumerable<TEntity>> GetAllAsync(params Expression<Func<TEntity, object>>[] loadProperties)
        {
            return await GetQueryable(loadProperties).ExecuteAsync();
        }

        public async Task<IEnumerable<TEntity>> GetAllAsync(ISorting[] sortingColumns)
        {
            return await GetAllAsync(sortingColumns, new Expression<Func<TEntity, object>>[0]);
        }

        public async Task<IEnumerable<TEntity>> GetAllAsync(ISorting[] sortingColumns, params string[] loadProperties)
        {
            var query = GetQueryable(loadProperties);

            if (sortingColumns != null && sortingColumns.Length > 0)
            {
                query = (CqlQuery<TEntity>)query.OrderBy(sortingColumns);
            }
            return await query.ExecuteAsync();
        }

        public async Task<IEnumerable<TEntity>> GetAllAsync(ISorting[] sortingColumns, params Expression<Func<TEntity, object>>[] loadProperties)
        {
            return await GetAllAsync(sortingColumns, GetPropertyNames(loadProperties));
        }

        public async Task<TEntity> GetAsync(TKey id, bool force)
        {
            return await GetAsync(id, force, new string[0]);
        }

        public async Task<TEntity> GetAsync(TKey id)
        {
            return await GetAsync(id, false, new string[0]);
        }

        public async Task<TEntity> GetAsync(TKey id, params string[] loadProperties)
        {
            return await GetAsync(id, false, loadProperties);
        }

        public async Task<TEntity> GetAsync(TKey id, bool force, params string[] loadProperties)
        {
            if (id != null)
            {
                var item = await GetSet().FindAsync(id);
                if (item != null)
                {
                    if (loadProperties != null && loadProperties.Length > 0)
                    {
                        foreach (var property in loadProperties)
                        {
                            if (!string.IsNullOrEmpty(property))
                            {
                                var props = property.Split('.');
                                if (props.Length == 1)
                                {
                                    await UnitOfWork.LoadPropertyAsync(item, property);
                                }
                                else
                                {
                                    await LoadCascadeAsync(props, item);
                                }
                            }
                        }
                    }
                    if (force) UnitOfWork.Reload(item);
                }
                return item;
            }
            else
                return null;
        }

        public async Task<TEntity> GetAsync(TKey id, params Expression<Func<TEntity, object>>[] loadProperties)
        {
            return await GetAsync(id, false, GetPropertyNames(loadProperties));
        }

        public async Task<TEntity> GetAsync(TKey id, bool force, params Expression<Func<TEntity, object>>[] loadProperties)
        {
            return await GetAsync(id, force, GetPropertyNames(loadProperties));
        }

        public async Task<IEnumerable<TEntity>> GetFilteredAsync(Expression<Func<TEntity, bool>> filter)
        {
            return await GetFilteredAsync(filter, new Expression<Func<TEntity, object>>[0]);
        }

        public async Task<IEnumerable<TEntity>> GetFilteredAsync(Expression<Func<TEntity, bool>> filter, params string[] loadProperties)
        {
            if (filter == null)
                throw new ArgumentException("Filter expression cannot be null", nameof(filter));

            return await GetQueryable(loadProperties).Where(filter).ExecuteAsync();
        }

        public async Task<IEnumerable<TEntity>> GetFilteredAsync(Expression<Func<TEntity, bool>> filter, params Expression<Func<TEntity, object>>[] loadProperties)
        {
            if (filter == null)
                throw new ArgumentException("Filter expression cannot be null", nameof(filter));

            return await GetQueryable(loadProperties).Where(filter).ExecuteAsync();
        }

        public async Task<IEnumerable<TEntity>> GetFilteredAsync(Expression<Func<TEntity, bool>> filter, ISorting[] sortColumns)
        {
            return await GetFilteredAsync(filter, sortColumns, new Expression<Func<TEntity, object>>[0]);
        }

        public async Task<IEnumerable<TEntity>> GetFilteredAsync(Expression<Func<TEntity, bool>> filter, ISorting[] sortColumns, params string[] loadProperties)
        {
            if (filter == null)
                throw new ArgumentException("Filter expression cannot be null", nameof(filter));

            var query = GetQueryable(loadProperties).Where(filter);
            if (sortColumns != null && sortColumns.Length > 0)
            {
                query = (CqlQuery<TEntity>)query.OrderBy(sortColumns);
            }
            return await query.ExecuteAsync();
        }

        public async Task<IEnumerable<TEntity>> GetFilteredAsync(Expression<Func<TEntity, bool>> filter, ISorting[] sortColumns, params Expression<Func<TEntity, object>>[] loadProperties)
        {
            return await GetFilteredAsync(filter, sortColumns, GetPropertyNames(loadProperties));
        }

        public async Task<TEntity> GetFirstAsync(Expression<Func<TEntity, bool>> filter)
        {
            return await GetFirstAsync(filter, new Expression<Func<TEntity, object>>[0]);
        }

        public async Task<TEntity> GetFirstAsync(Expression<Func<TEntity, bool>> filter, params string[] loadProperties)
        {
            return await GetQueryable(loadProperties).Where(filter).FirstOrDefault().ExecuteAsync();
        }

        public async Task<TEntity> GetFirstAsync(Expression<Func<TEntity, bool>> filter, params Expression<Func<TEntity, object>>[] loadProperties)
        {
            return await GetQueryable(loadProperties).Where(filter).FirstOrDefault().ExecuteAsync();
        }

        public async Task<TEntity> GetFirstAsync(Expression<Func<TEntity, bool>> filter, ISorting[] sortingColumns, params string[] loadProperties)
        {
            return await ((CqlQuery<TEntity>)GetQueryable(loadProperties).OrderBy(sortingColumns)).Where(filter).FirstOrDefault().ExecuteAsync();
        }

        public async Task<TEntity> GetFirstAsync(Expression<Func<TEntity, bool>> filter, ISorting[] sortingColumns, params Expression<Func<TEntity, object>>[] loadProperties)
        {
            return await ((CqlQuery<TEntity>)GetQueryable(loadProperties).OrderBy(sortingColumns)).Where(filter).FirstOrDefault().ExecuteAsync();
        }

        public async Task<IEnumerable<TEntity>> GetPagedAsync(int limit, ISorting[] sortColumns)
        {
            return await GetPagedAsync(1, limit, sortColumns, new Expression<Func<TEntity, object>>[0]);
        }

        public async Task<IEnumerable<TEntity>> GetPagedAsync(int limit, ISorting[] sortColumns, params string[] loadProperties)
        {
            return await GetPagedAsync((ISpecification<TEntity>)null, 1, limit, sortColumns, loadProperties);
        }

        public async Task<IEnumerable<TEntity>> GetPagedAsync(int limit, ISorting[] sortColumns, params Expression<Func<TEntity, object>>[] loadProperties)
        {
            return await GetPagedAsync((ISpecification<TEntity>)null, 1, limit, sortColumns, loadProperties);
        }

        public async Task<IEnumerable<TEntity>> GetPagedAsync(ISpecification<TEntity> specification, int limit, ISorting[] sortColumns)
        {
            return await GetPagedAsync(specification, 1, limit, sortColumns, new Expression<Func<TEntity, object>>[0]);
        }

        public async Task<IEnumerable<TEntity>> GetPagedAsync(ISpecification<TEntity> specification, int limit, ISorting[] sortColumns, params string[] loadProperties)
        {
            return await GetPagedAsync(specification, 1, limit, sortColumns, loadProperties);
        }

        public async Task<IEnumerable<TEntity>> GetPagedAsync(ISpecification<TEntity> specification, int limit, ISorting[] sortColumns, params Expression<Func<TEntity, object>>[] loadProperties)
        {
            return await GetPagedAsync(specification, 1, limit, sortColumns, loadProperties);
        }

        public async Task<IEnumerable<TEntity>> GetPagedAsync(Expression<Func<TEntity, bool>> filter, int limit, ISorting[] sortColumns)
        {
            return await GetPagedAsync(filter, 1, limit, sortColumns, new Expression<Func<TEntity, object>>[0]);
        }

        public async Task<IEnumerable<TEntity>> GetPagedAsync(Expression<Func<TEntity, bool>> filter, int limit, ISorting[] sortColumns, params string[] loadProperties)
        {
            return await GetPagedAsync(filter, 1, limit, sortColumns, loadProperties);
        }

        public async Task<IEnumerable<TEntity>> GetPagedAsync(Expression<Func<TEntity, bool>> filter, int limit, ISorting[] sortColumns, params Expression<Func<TEntity, object>>[] loadProperties)
        {
            return await GetPagedAsync(filter, 1, limit, sortColumns, loadProperties);
        }

        public async Task<IEnumerable<TEntity>> GetPagedAsync(int pageIndex, int pageCount, ISorting[] sortColumns)
        {
            return await GetPagedAsync(pageIndex, pageCount, sortColumns, new Expression<Func<TEntity, object>>[0]);
        }

        public async Task<IEnumerable<TEntity>> GetPagedAsync(int pageIndex, int pageCount, ISorting[] sortColumns, params string[] loadProperties)
        {
            return await GetPagedAsync((ISpecification<TEntity>)null, pageIndex, pageCount, sortColumns, loadProperties);
        }

        public async Task<IEnumerable<TEntity>> GetPagedAsync(int pageIndex, int pageCount, ISorting[] sortColumns, params Expression<Func<TEntity, object>>[] loadProperties)
        {
            return await GetPagedAsync((ISpecification<TEntity>)null, pageIndex, pageCount, sortColumns, loadProperties);
        }

        public async Task<IEnumerable<TEntity>> GetPagedAsync(ISpecification<TEntity> specification, int pageIndex, int pageCount, ISorting[] sortColumns)
        {
            return await GetPagedAsync(specification, pageIndex, pageCount, sortColumns, new Expression<Func<TEntity, object>>[0]);
        }

        public async Task<IEnumerable<TEntity>> GetPagedAsync(ISpecification<TEntity> specification, int pageIndex, int pageCount, ISorting[] sortColumns,
            params string[] loadProperties)
        {
            return await GetPagedAsync(specification?.SatisfiedBy(), pageIndex, pageCount, sortColumns, loadProperties);
        }

        public async Task<IEnumerable<TEntity>> GetPagedAsync(ISpecification<TEntity> specification, int pageIndex, int pageCount, ISorting[] sortColumns,
            params Expression<Func<TEntity, object>>[] loadProperties)
        {
            return await GetPagedAsync(specification?.SatisfiedBy(), pageIndex, pageCount, sortColumns, loadProperties);
        }

        public async Task<IEnumerable<TEntity>> GetPagedAsync(Expression<Func<TEntity, bool>> filter, int pageIndex, int pageCount, ISorting[] sortColumns)
        {
            return await GetPagedAsync(filter, pageIndex, pageCount, sortColumns, new Expression<Func<TEntity, object>>[0]);
        }

        public async Task<IEnumerable<TEntity>> GetPagedAsync(Expression<Func<TEntity, bool>> filter, int pageIndex, int pageCount, ISorting[] sortColumns,
            params string[] loadProperties)
        {
            var query = filter != null ? GetQueryable(loadProperties).Where(filter) : GetQueryable(loadProperties);

            if (sortColumns != null && sortColumns.Length > 0)
            {
                query = (CqlQuery<TEntity>)query.OrderBy(sortColumns);
            }
            if (pageCount > 0)
                return query.Skip((pageIndex - 1) * pageCount).Take(pageCount);

            return await query.ExecuteAsync();
        }

        public async Task<IEnumerable<TEntity>> GetPagedAsync(Expression<Func<TEntity, bool>> filter, int pageIndex, int pageCount, ISorting[] sortColumns,
            params Expression<Func<TEntity, object>>[] loadProperties)
        {
            return await GetPagedAsync(filter, pageIndex, pageCount, sortColumns, GetPropertyNames(loadProperties));
        }

        public async Task<TEntity> GetSingleAsync(Expression<Func<TEntity, bool>> filter)
        {
            return await GetSingleAsync(filter, new Expression<Func<TEntity, object>>[0]);
        }

        public async Task<TEntity> GetSingleAsync(Expression<Func<TEntity, bool>> filter, params string[] loadProperties)
        {
            return await GetQueryable(loadProperties).Where(filter).FirstOrDefault().ExecuteAsync();
        }

        public async Task<TEntity> GetSingleAsync(Expression<Func<TEntity, bool>> filter, params Expression<Func<TEntity, object>>[] loadProperties)
        {
            return await GetQueryable(loadProperties).Where(filter).FirstOrDefault().ExecuteAsync();
        }

        public async Task<TEntity> GetSingleAsync(Expression<Func<TEntity, bool>> filter, ISorting[] sortingColumns, params string[] loadProperties)
        {
            return await ((CqlQuery<TEntity>)GetQueryable(loadProperties).OrderBy(sortingColumns)).Where(filter).FirstOrDefault().ExecuteAsync();
        }

        public async Task<TEntity> GetSingleAsync(Expression<Func<TEntity, bool>> filter, ISorting[] sortingColumns, params Expression<Func<TEntity, object>>[] loadProperties)
        {
            return await ((CqlQuery<TEntity>)GetQueryable(loadProperties).OrderBy(sortingColumns)).Where(filter).FirstOrDefault().ExecuteAsync();
        }

        public async Task<TEntity> GetSingleAsync(Expression<Func<TEntity, bool>> filter, ISorting[] sortingColumns)
        {
            return await GetSingleAsync(filter, sortingColumns, (string)null);
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

        protected async Task LoadCascadeAsync(string[] props, object obj, int index = 0)
        {
#if NETSTANDARD1_6 || NETSTANDARD2_0
            var prop = obj.GetType().GetTypeInfo().GetDeclaredProperty(props[index]);
#else
            var prop = obj.GetType().GetProperty(props[index]);
#endif
            var nextObj = prop?.GetValue(obj);
            if (nextObj == null)
            {
                await UnitOfWork.LoadPropertyAsync(obj, props[index]);
                nextObj = prop?.GetValue(obj);
            }

            if (props.Length > index + 1) LoadCascade(props, nextObj, index + 1);
        }
    }
}