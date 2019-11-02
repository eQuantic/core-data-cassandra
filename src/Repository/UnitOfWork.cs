using System;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data.Linq;
using eQuantic.Core.Data.Repository;

namespace eQuantic.Core.Data.Cassandra.Repository
{
    public abstract class UnitOfWork : IQueryableUnitOfWork
    {
        private readonly ISession session;
        private Batch batch = null;
        private int count = 0;

        public UnitOfWork(ISession session)
        {
            this.session = session;
        }

        public void AppendCommand(CqlCommand command)
        {
            if (batch == null) batch = this.session.CreateBatch();
            if (this.batch.IsEmpty) count = 1;
            else count++;
            this.batch.Append(command);
        }

        public int Commit()
        {
            this.batch?.Execute();
            return this.count;
        }

        public int CommitAndRefreshChanges()
        {
            return Commit();
        }

        public async Task<int> CommitAndRefreshChangesAsync()
        {
            return await CommitAsync();
        }

        public async Task<int> CommitAsync()
        {
            await this.batch?.ExecuteAsync();
            return this.count;
        }

        Data.Repository.ISet<TEntity> IQueryableUnitOfWork.CreateSet<TEntity>()
        {
            return new Set<TEntity>(this.session);
        }

        public void Dispose()
        {
            this.session?.Dispose();
        }

        public abstract TRepository GetRepository<TRepository>() where TRepository : IRepository;

        public abstract TRepository GetRepository<TRepository>(string name) where TRepository : IRepository;

        public void RollbackChanges()
        {
            this.batch = null;
        }
    }
}