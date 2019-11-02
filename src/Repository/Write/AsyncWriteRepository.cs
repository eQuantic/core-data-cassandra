using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using Cassandra.Data.Linq;
using eQuantic.Core.Data.Repository;
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
            await AppendOrExecuteCommandAsync(GetSet().Insert(item));
        }

        public async Task<int> DeleteManyAsync(Expression<Func<TEntity, bool>> filter)
        {
            await AppendOrExecuteCommandAsync(GetSet().Where(filter).Delete());
            return 0;
        }

        public async Task<int> DeleteManyAsync(ISpecification<TEntity> specification)
        {
            return await DeleteManyAsync(specification.SatisfiedBy());
        }

        public async Task MergeAsync(TEntity persisted, TEntity current)
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

            await AppendOrExecuteCommandAsync(GetSet().Where(expression).Select(o => persisted).Update());
        }

        public async Task ModifyAsync(TEntity item)
        {
            if (item == (TEntity)null) return;

            var key = GetSet().GetKeyValue<TKey>(item);
            var expression = GetSet().GetKeyExpression(key);

            await AppendOrExecuteCommandAsync(GetSet().Where(expression).Select(o => item).Update());
        }

        public async Task RemoveAsync(TEntity item)
        {
            if (item == (TEntity)null) return;

            var key = GetSet().GetKeyValue<TKey>(item);
            var expression = GetSet().GetKeyExpression(key);
            await AppendOrExecuteCommandAsync(GetSet().Where(expression).Delete());
        }

        public async Task<int> UpdateManyAsync(Expression<Func<TEntity, bool>> filter, Expression<Func<TEntity, TEntity>> updateFactory)
        {
            await AppendOrExecuteCommandAsync(GetSet().Where(filter).Select(updateFactory).Update());
            return 0;
        }

        public async Task<int> UpdateManyAsync(ISpecification<TEntity> specification, Expression<Func<TEntity, TEntity>> updateFactory)
        {
            return await UpdateManyAsync(specification.SatisfiedBy(), updateFactory);
        }

        private async Task AppendOrExecuteCommandAsync(CqlCommand command)
        {
            var unitOfWork = UnitOfWork as UnitOfWork;
            if (unitOfWork != null) unitOfWork.AppendCommand(command);
            else await command.ExecuteAsync();
        }
    }
}