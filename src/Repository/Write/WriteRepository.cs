using System;
using System.Linq;
using System.Linq.Expressions;
using Cassandra.Data.Linq;
using eQuantic.Core.Data.Repository;
using eQuantic.Core.Data.Repository.Sql;
using eQuantic.Core.Data.Repository.Write;
using eQuantic.Core.Linq.Specification;
using System;
using System.Linq;

namespace eQuantic.Core.Data.EntityFramework.Repository.Write
{
    public class WriteRepository<TUnitOfWork, TEntity, TKey> : IWriteRepository<TUnitOfWork, TEntity, TKey>
        where TUnitOfWork : IQueryableUnitOfWork
        where TEntity : class, IEntity, new()
    {
        private Set<TEntity> _dbset = null;

        public WriteRepository(TUnitOfWork unitOfWork)
        {
            if (unitOfWork == null)
                throw new ArgumentNullException(nameof(unitOfWork));

            UnitOfWork = unitOfWork;
        }

        public TUnitOfWork UnitOfWork { get; set; }

        public void Add(TEntity item)
        {
            if (item != (TEntity)null)
                GetSet().Insert(item);
        }

        public int DeleteMany(Expression<Func<TEntity, bool>> filter)
        {
            GetSet().Where(filter).Delete().Execute();
            return 0;
        }

        public int DeleteMany(ISpecification<TEntity> specification)
        {
            return DeleteMany(specification.SatisfiedBy());
        }

        /// <summary>
        /// <see cref="M:System.IDisposable.Dispose"/>
        /// </summary>
        public void Dispose()
        {
            UnitOfWork?.Dispose();
        }

        public void Merge(TEntity persisted, TEntity current)
        {
            UnitOfWork.ApplyCurrentValues(persisted, current);
        }

        public void Modify(TEntity item)
        {
            if (item != (TEntity)null)
                UnitOfWork.SetModified(item);
        }

        public void Remove(TEntity item)
        {
            if (item != (TEntity)null)
            {
                //attach item if not exist
                UnitOfWork.Attach(item);

                //set as "removed"
                GetSet().Remove(item);
            }
        }

        public void TrackItem(TEntity item)
        {
            if (item != (TEntity)null)
                UnitOfWork.Attach<TEntity>(item);
        }

        public int UpdateMany(Expression<Func<TEntity, bool>> filter, Expression<Func<TEntity, TEntity>> updateFactory)
        {
            GetSet().Where(filter).Select(updateFactory).Update().Execute();
            return 0;
        }

        public int UpdateMany(ISpecification<TEntity> specification, Expression<Func<TEntity, TEntity>> updateFactory)
        {
            return UpdateMany(specification.SatisfiedBy(), updateFactory);
        }

        protected Set<TEntity> GetSet()
        {
            return _dbset ?? (_dbset = (Set<TEntity>)UnitOfWork.CreateSet<TEntity>());
        }
    }
}