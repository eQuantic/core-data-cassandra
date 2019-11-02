using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Cassandra.Data.Linq;
using eQuantic.Core.Data.Cassandra.Repository.Read;
using eQuantic.Core.Data.Repository;
using eQuantic.Core.Linq.Specification;

namespace eQuantic.Core.Data.Cassandra.Repository
{
    /// <summary>
    /// Repository base class
    /// </summary>
    /// <typeparam name="TEntity">The type of underlying entity in this repository</typeparam>
    /// <typeparam name="TKey">
    /// <see cref="eQuantic.Core.Data.Repository.IRepository{TUnitOfWork, TEntity, TKey}"/>
    /// </typeparam>
    public class Repository<TUnitOfWork, TEntity, TKey> : ReadRepository<TUnitOfWork, TEntity, TKey>, IRepository<TUnitOfWork, TEntity, TKey>
        where TUnitOfWork : IQueryableUnitOfWork
        where TEntity : class, IEntity, new()
    {
        #region Constructor

        /// <summary>
        /// Create a new instance of repository
        /// </summary>
        /// <param name="unitOfWork">Associated Unit Of Work</param>
        public Repository(TUnitOfWork unitOfWork) : base(unitOfWork)
        {
        }

        #endregion Constructor

        #region IRepository Members

        /// <summary>
        /// <see cref="eQuantic.Core.Data.Repository.IRepository{TUnitOfWork, TEntity, TKey}"/>
        /// </summary>
        /// <param name="item">
        /// <see cref="eQuantic.Core.Data.Repository.IRepository{TUnitOfWork, TEntity, TKey}"/>
        /// </param>
        public virtual void Add(TEntity item)
        {
            if (item == (TEntity)null) return;
            AppendOrExecuteCommand(GetSet().Insert(item));
        }

        /// <summary>
        /// <see cref="eQuantic.Core.Data.Repository.IRepository{TUnitOfWork, TEntity, TKey}"/>
        /// </summary>
        /// <param name="filter">
        /// <see cref="eQuantic.Core.Data.Repository.IRepository{TUnitOfWork, TEntity, TKey}"/>
        /// </param>
        /// <returns></returns>
        public int DeleteMany(Expression<Func<TEntity, bool>> filter)
        {
            AppendOrExecuteCommand(GetSet().Where(filter).Delete());
            return 0;
        }

        /// <summary>
        /// <see cref="eQuantic.Core.Data.Repository.IRepository{TUnitOfWork, TEntity, TKey}"/>
        /// </summary>
        /// <param name="specification">
        /// <see cref="eQuantic.Core.Data.Repository.IRepository{TUnitOfWork, TEntity, TKey}"/>
        /// </param>
        /// <returns></returns>
        public int DeleteMany(ISpecification<TEntity> specification)
        {
            return DeleteMany(specification.SatisfiedBy());
        }

        /// <summary>
        /// <see cref="eQuantic.Core.Data.Repository.IRepository{TUnitOfWork, TEntity, TKey}"/>
        /// </summary>
        /// <param name="persisted">
        /// <see cref="eQuantic.Core.Data.Repository.IRepository{TUnitOfWork, TEntity, TKey}"/>
        /// </param>
        /// <param name="current">
        /// <see cref="eQuantic.Core.Data.Repository.IRepository{TUnitOfWork, TEntity, TKey}"/>
        /// </param>
        public virtual void Merge(TEntity persisted, TEntity current)
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

        /// <summary>
        /// <see cref="eQuantic.Core.Data.Repository.IRepository{TUnitOfWork, TEntity, TKey}"/>
        /// </summary>
        /// <param name="item">
        /// <see cref="eQuantic.Core.Data.Repository.IRepository{TUnitOfWork, TEntity, TKey}"/>
        /// </param>
        public virtual void Modify(TEntity item)
        {
            if (item == (TEntity)null) return;

            var key = GetSet().GetKeyValue<TKey>(item);
            var expression = GetSet().GetKeyExpression(key);

            AppendOrExecuteCommand(GetSet().Where(expression).Select(o => item).Update());
        }

        /// <summary>
        /// <see cref="eQuantic.Core.Data.Repository.IRepository{TUnitOfWork, TEntity, TKey}"/>
        /// </summary>
        /// <param name="item">
        /// <see cref="eQuantic.Core.Data.Repository.IRepository{TUnitOfWork, TEntity, TKey}"/>
        /// </param>
        public virtual void Remove(TEntity item)
        {
            if (item == (TEntity)null) return;

            var key = GetSet().GetKeyValue<TKey>(item);
            var expression = GetSet().GetKeyExpression(key);
            AppendOrExecuteCommand(GetSet().Where(expression).Delete());
        }

        /// <summary>
        /// <see cref="eQuantic.Core.Data.Repository.IRepository{TUnitOfWork, TEntity, TKey}"/>
        /// </summary>
        /// <param name="item">
        /// <see cref="eQuantic.Core.Data.Repository.IRepository{TUnitOfWork, TEntity, TKey}"/>
        /// </param>
        public virtual void TrackItem(TEntity item)
        {
            if (item == (TEntity)null) return;
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
        public int UpdateMany(Expression<Func<TEntity, bool>> filter, Expression<Func<TEntity, TEntity>> updateFactory)
        {
            AppendOrExecuteCommand(GetSet().Where(filter).Select(updateFactory).Update());
            return 0;
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
        public int UpdateMany(ISpecification<TEntity> specification, Expression<Func<TEntity, TEntity>> updateFactory)
        {
            return UpdateMany(specification.SatisfiedBy(), updateFactory);
        }

        #endregion IRepository Members

        private void AppendOrExecuteCommand(CqlCommand command)
        {
            var unitOfWork = UnitOfWork as UnitOfWork;
            if (unitOfWork != null) unitOfWork.AppendCommand(command);
            else command.Execute();
        }
    }
}