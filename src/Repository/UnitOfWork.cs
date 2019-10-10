using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data.Linq;
using Cassandra.Mapping;
using eQuantic.Core.Data.Repository;
using eQuantic.Core.Data.Repository.Sql;

namespace eQuantic.Core.Data.EntityFramework.Repository
{
    public abstract class UnitOfWork : IQueryableUnitOfWork
    {
        public static int IsMigrating = 0;
        private readonly ISession _session;
        private readonly List<CqlCommand> _commands = new List<CqlCommand>();

        protected UnitOfWork(ISession session)
        {
            _session = session;
        }

        public virtual void Dispose()
        {
            _session?.Dispose();
        }

        public abstract TRepository GetRepository<TRepository>() where TRepository : IRepository;

        public abstract TRepository GetRepository<TRepository>(string name) where TRepository : IRepository;

        public void BeginTransaction()
        {
            _transaction?.Dispose();
            _transaction = _context.Database.BeginTransaction();
        }

        public int Commit()
        {
            var i = 0;
            foreach (var c in _commands)
            {
                c.Execute();
                i++;
            }
            _commands.Clear();
            return i;
        }

        public async Task<int> CommitAsync()
        {
            var i = 0;
            foreach (var c in _commands)
            {
                await c.ExecuteAsync();
                i++;
            }
            _commands.Clear();
            return i;
        }

        public int CommitAndRefreshChanges()
        {
            int changes = 0;
            bool saveFailed = false;

            do
            {
                try
                {
                    changes = _context.SaveChanges();

                    saveFailed = false;

                }
                catch (DbUpdateConcurrencyException ex)
                {
                    saveFailed = true;

                    ex.Entries.ToList()
                              .ForEach(entry => entry.OriginalValues.SetValues(entry.GetDatabaseValues()));

                }
            } while (saveFailed);

            return changes;
        }

        public async Task<int> CommitAndRefreshChangesAsync()
        {
            int changes = 0;
            bool saveFailed = false;

            do
            {
                try
                {
                    changes = await _context.SaveChangesAsync();

                    saveFailed = false;

                }
                catch (DbUpdateConcurrencyException ex)
                {
                    saveFailed = true;

                    ex.Entries.ToList()
                        .ForEach(entry => entry.OriginalValues.SetValues(entry.GetDatabaseValues()));

                }
            } while (saveFailed);

            return changes;
        }

        public void RollbackChanges()
        {
            _commands.Clear();
        }

        private string GetQueryParameters(params object[] parameters)
        {
            var cmd = "";
            if (parameters != null && parameters.Length > 0)
            {
                for (int i = 0; i < parameters.Length; i++)
                {
                    if (i > 0) cmd += ",";

                    if (parameters[i] == null) cmd += " NULL";
                    else
                    {
                        var fmt = " {0}";
                        if (parameters[i] is Guid || parameters[i] is String || parameters[i] is DateTime)
                            fmt = " '{0}'";

                        cmd += string.Format(fmt, parameters[i]);
                    }
                }
            }
            return cmd;
        }

        private string GetQueryProcedure(string name, params object[] parameters)
        {
            return $"EXEC {name}{GetQueryParameters(parameters)}";
        }

        private string GetQueryFunction(string name, params object[] parameters)
        {
            return $"SELECT {name}({GetQueryParameters(parameters)} )";
        }

        public int ExecuteProcedure(string name, params object[] parameters)
        {
            return ExecuteCommand(GetQueryProcedure(name, parameters) + ";");
        }

        public async Task<int> ExecuteProcedureAsync(string name, params object[] parameters)
        {
            return await ExecuteCommandAsync(GetQueryProcedure(name, parameters) + ";");
        }

        public TResult ExecuteFunction<TResult>(string name, params object[] parameters) where TResult : class
        {
            
        }

        public async Task<TResult> ExecuteFunctionAsync<TResult>(string name, params object[] parameters) where TResult : class
        {
            
        }


        public IEnumerable<TEntity> ExecuteQuery<TEntity>(string cqlQuery, params object[] parameters) where TEntity : class
        {
            IMapper mapper = new Mapper(GetSession());
            return mapper.Fetch<TEntity>(cqlQuery, parameters);
        }

        public int ExecuteCommand(string cqlCommand, params object[] parameters)
        {
            var statement = GetSession().Prepare(cqlCommand);
            var result = GetSession().Execute(statement.Bind(parameters));
            return result != null ? 1 : 0;
        }

        public async Task<int> ExecuteCommandAsync(string cqlCommand, params object[] parameters)
        {
            var statement = GetSession().Prepare(cqlCommand);
            var result = await GetSession().ExecuteAsync(statement.Bind(parameters));
            return result != null ? 1 : 0;
        }

        public void Attach<TEntity>(TEntity item) where TEntity : class
        {
            
        }

        public void Reload<TEntity>(TEntity item) where TEntity : class
        {
            
        }

        public void SetModified<TEntity>(TEntity item) where TEntity : class
        {
            
        }

        public void ApplyCurrentValues<TEntity>(TEntity original, TEntity current) where TEntity : class
        {
            
        }

        public void LoadCollection<TEntity, TElement>(TEntity item, Expression<Func<TEntity, IEnumerable<TElement>>> navigationProperty, Expression<Func<TElement, bool>> filter = null) where TEntity : class where TElement : class
        {
            
        }

        public async Task LoadCollectionAsync<TEntity, TElement>(TEntity item, Expression<Func<TEntity, IEnumerable<TElement>>> navigationProperty, Expression<Func<TElement, bool>> filter = null) where TEntity : class where TElement : class
        {
            
        }

        public void LoadProperty<TEntity, TComplexProperty>(TEntity item, Expression<Func<TEntity, TComplexProperty>> selector) where TEntity : class where TComplexProperty : class
        {
            
        }

        public async Task LoadPropertyAsync<TEntity, TComplexProperty>(TEntity item, Expression<Func<TEntity, TComplexProperty>> selector) where TEntity : class where TComplexProperty : class
        {
            
        }

        public void LoadProperty<TEntity>(TEntity item, string propertyName) where TEntity : class
        {
            
        }

        public async Task LoadPropertyAsync<TEntity>(TEntity item, string propertyName) where TEntity : class
        {
            await _context.Entry<TEntity>(item).Reference(propertyName).LoadAsync();
        }

        public void UpdateDatabase()
        {
            if (0 == Interlocked.Exchange(ref IsMigrating, 1))
            {
                try
                {
                    _context.Database.Migrate();
                }
                finally
                {
                    Interlocked.Exchange(ref IsMigrating, 0);
                }
            }
        }

        public IEnumerable<string> GetPendingMigrations()
        {
            return _context.Database.GetPendingMigrations();
        }

        Data.Repository.ISet<TEntity> IQueryableUnitOfWork.CreateSet<TEntity>()
        {
            return new Set<TEntity>(GetSession());
        }

        public ISession GetSession()
        {
            return _session ?? (_session = _cluster.Connect(_connectionString.DefaultKeyspace));
        }
    }
}
