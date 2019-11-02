using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Cassandra.Data.Linq;
using eQuantic.Core.Data.Repository;
using eQuantic.Core.Data.Repository.Write;
using eQuantic.Core.Linq.Specification;

namespace eQuantic.Core.Data.Cassandra.Repository.Write
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
            if (item == (TEntity)null) return;

            AppendOrExecuteCommand(GetSet().Insert(item));
        }

        public int DeleteMany(Expression<Func<TEntity, bool>> filter)
        {
            AppendOrExecuteCommand(GetSet().Where(filter).Delete());
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

            var key = GetSet().GetKeyValue<TKey>(persisted);
            var expression = GetSet().GetKeyExpression(key);

            AppendOrExecuteCommand(GetSet().Where(expression).Select(o => persisted).Update());
        }

        public void Modify(TEntity item)
        {
            if (item == (TEntity)null) return;

            var key = GetSet().GetKeyValue<TKey>(item);
            var expression = GetSet().GetKeyExpression(key);

            AppendOrExecuteCommand(GetSet().Where(expression).Select(o => item).Update());
        }

        public void Remove(TEntity item)
        {
            if (item == (TEntity)null) return;

            var key = GetSet().GetKeyValue<TKey>(item);
            var expression = GetSet().GetKeyExpression(key);
            AppendOrExecuteCommand(GetSet().Where(expression).Delete());
        }

        public void TrackItem(TEntity item)
        {
            if (item == (TEntity)null) return;
        }

        public int UpdateMany(Expression<Func<TEntity, bool>> filter, Expression<Func<TEntity, TEntity>> updateFactory)
        {
            AppendOrExecuteCommand(GetSet().Where(filter).Select(updateFactory).Update());
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

        private void AppendOrExecuteCommand(CqlCommand command)
        {
            var unitOfWork = UnitOfWork as UnitOfWork;
            if (unitOfWork != null) unitOfWork.AppendCommand(command);
            else command.Execute();
        }
    }
}