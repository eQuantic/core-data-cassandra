using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Cassandra.Data.Linq;
using eQuantic.Core.Data.Repository;
using eQuantic.Core.Data.Repository.Read;
using eQuantic.Core.Data.Repository.Sql;
using eQuantic.Core.Linq;
using eQuantic.Core.Linq.Extensions;
using eQuantic.Core.Linq.Specification;

namespace eQuantic.Core.Data.EntityFramework.Repository.Read
{
    public class ReadRepository<TUnitOfWork, TEntity, TKey> : IReadRepository<TUnitOfWork, TEntity, TKey>
        where TUnitOfWork : IQueryableUnitOfWork
        where TEntity : class, IEntity, new()
    {
        private Set<TEntity> _dbSet = null;

        /// <summary>
        /// Create a new instance of repository
        /// </summary>
        /// <param name="unitOfWork">Associated Unit Of Work</param>
        public ReadRepository(TUnitOfWork unitOfWork)
        {
            if (unitOfWork == null)
                throw new ArgumentNullException(nameof(unitOfWork));

            UnitOfWork = unitOfWork;
        }

        /// <summary>
        /// <see cref="eQuantic.Core.Data.Repository.Read.IReadRepository{TUnitOfWork, TEntity, TKey}"/>
        /// </summary>
        public TUnitOfWork UnitOfWork { get; private set; }

        public IEnumerable<TEntity> AllMatching(ISpecification<TEntity> specification)
        {
            return GetSet().Where(specification.SatisfiedBy()).Execute();
        }

        /// <summary>
        /// <see cref="eQuantic.Core.Data.Repository.Read.IReadRepository{TUnitOfWork, TEntity, TKey}"/>
        /// </summary>
        /// <returns></returns>
        public long Count()
        {
            return GetSet().Count().Execute();
        }

        /// <summary>
        /// <see cref="eQuantic.Core.Data.Repository.Read.IReadRepository{TUnitOfWork, TEntity, TKey}"/>
        /// </summary>
        /// <param name="specification"><see cref="eQuantic.Core.Data.Repository.Read.IReadRepository{TUnitOfWork, TEntity, TKey}"/></param>
        /// <returns></returns>
        public long Count(ISpecification<TEntity> specification)
        {
            return GetSet().Where(specification.SatisfiedBy()).Count().Execute();
        }

        /// <summary>
        /// <see cref="eQuantic.Core.Data.Repository.Read.IReadRepository{TUnitOfWork, TEntity, TKey}"/>
        /// </summary>
        /// <param name="filter"><see cref="eQuantic.Core.Data.Repository.Read.IReadRepository{TUnitOfWork, TEntity, TKey}"/></param>
        /// <returns></returns>
        public long Count(Expression<Func<TEntity, bool>> filter)
        {
            return GetSet().Where(filter).Count().Execute();
        }

        /// <summary>
        /// <see cref="M:System.IDisposable.Dispose"/>
        /// </summary>
        public void Dispose()
        {
            UnitOfWork?.Dispose();
        }

        public TEntity Get(TKey id)
        {
            return id != null ? GetSet().Find(id) : null;
        }

        public IEnumerable<TEntity> GetAll()
        {
            return GetSet().Execute();
        }

        public IEnumerable<TEntity> GetAll(ISorting[] sortingColumns)
        {
            return ((CqlQuery<TEntity>)GetSet().OrderBy(sortingColumns)).Execute();
        }

        public IEnumerable<TEntity> GetFiltered(Expression<Func<TEntity, bool>> filter)
        {
            return GetFiltered(filter, null);
        }

        public IEnumerable<TEntity> GetFiltered(Expression<Func<TEntity, bool>> filter, ISorting[] sortColumns)
        {
            if (filter == null)
                throw new ArgumentException("Filter expression cannot be null", nameof(filter));

            CqlQuery<TEntity> query = GetSet().Where(filter);
            if (sortColumns != null && sortColumns.Length > 0)
            {
                query = (CqlQuery<TEntity>)query.OrderBy(sortColumns);
            }
            return query.Execute();
        }

        public TEntity GetFirst(Expression<Func<TEntity, bool>> filter)
        {
            return GetSet().FirstOrDefault(filter).Execute();
        }

        public IEnumerable<TEntity> GetPaged(int limit, ISorting[] sortColumns)
        {
            return GetPaged((Expression<Func<TEntity, bool>>)null, 1, limit, sortColumns);
        }

        public IEnumerable<TEntity> GetPaged(ISpecification<TEntity> specification, int limit, ISorting[] sortColumns)
        {
            return GetPaged(specification.SatisfiedBy(), 1, limit, sortColumns);
        }

        public IEnumerable<TEntity> GetPaged(Expression<Func<TEntity, bool>> filter, int limit, ISorting[] sortColumns)
        {
            return GetPaged(filter, 1, limit, sortColumns);
        }

        public IEnumerable<TEntity> GetPaged(int pageIndex, int pageCount, ISorting[] sortColumns)
        {
            return GetPaged((Expression<Func<TEntity, bool>>)null, pageIndex, pageCount, sortColumns);
        }

        public IEnumerable<TEntity> GetPaged(ISpecification<TEntity> specification, int pageIndex, int pageCount, ISorting[] sortColumns)
        {
            return GetPaged(specification.SatisfiedBy(), pageIndex, pageCount, sortColumns);
        }

        public IEnumerable<TEntity> GetPaged(Expression<Func<TEntity, bool>> filter, int pageIndex, int pageCount, ISorting[] sortColumns)
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
                var pagingState = query.SetPageSize(skip).ExecutePaged().PagingState;
                return query.SetPagingState(pagingState).SetPageSize(pageCount).ExecutePaged();
            }

            return query.Execute();
        }

        public TEntity GetSingle(Expression<Func<TEntity, bool>> filter)
        {
            return GetSet().FirstOrDefault(filter).Execute();
        }

        protected Set<TEntity> GetSet()
        {
            return _dbSet ?? (_dbSet = (Set<TEntity>)UnitOfWork.CreateSet<TEntity>());
        }
    }
}