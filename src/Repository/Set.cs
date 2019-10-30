using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data.Linq;
using Cassandra.Mapping;
using eQuantic.Core.Data.Repository;

namespace eQuantic.Core.Data.Cassandra.Repository
{
    public class Set<TEntity> : Table<TEntity>, Data.Repository.ISet<TEntity> where TEntity : class, IEntity, new()
    {
        public Set(ISession session) : base(session)
        {
        }

        public TEntity Find<TKey>(TKey id)
        {
            var expression = GetKeyExpression(id);
            return this.Where(expression).FirstOrDefault().Execute();
        }

        public async Task<TEntity> FindAsync<TKey>(TKey id)
        {
            var expression = GetKeyExpression(id);
            return await this.Where(expression).FirstOrDefault().ExecuteAsync();
        }

        public virtual Expression<Func<TEntity, bool>> GetKeyExpression<TKey>(TKey key)
        {
            Expression<Func<TEntity, bool>> exp = null;

            var partitionKeys = GetPartitionKeys();
            if (partitionKeys.Any())
            {
                var values = GetKeyDictionary(key, partitionKeys);
                var arg = Expression.Parameter(typeof(TEntity), "item");
                foreach (var partitionKey in partitionKeys)
                {
                    var property = Expression.Property(arg, partitionKey);
                    var constant = Expression.Constant(values[partitionKey]);
                    var compare = Expression.Equal(property, constant);
                    if (exp == null)
                        exp = Expression.Lambda<Func<TEntity, bool>>(compare, arg);
                    else exp = Expression.Lambda<Func<TEntity, bool>>(Expression.AndAlso(exp.Body, compare), arg);
                }
            }
            return exp;
        }

        public virtual TKey GetKeyValue<TKey>(TEntity item)
        {
            var partitionKeys = GetPartitionKeys();
            var values = new Dictionary<string, object>();
            if (partitionKeys.Any())
            {
                var itemType = typeof(TEntity);
                foreach (var partitionKey in partitionKeys)
                {
                    var prop = itemType.GetProperty(partitionKey, BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance);
                    if (prop == null) continue;

                    values.Add(partitionKey, prop.GetValue(item));
                }
            }

            if (values.Count > 1) return GetKeyObject<TKey>(values);
            return (TKey)values.FirstOrDefault().Value;
        }

        private Dictionary<string, object> GetKeyDictionary<TKey>(TKey key, string[] propertyNames)
        {
            var keyType = typeof(TKey);
            var dict = new Dictionary<string, object>();
            var isObj = Convert.GetTypeCode(key) == TypeCode.Object;
            foreach (var propertyName in propertyNames)
            {
                var prop = keyType.GetProperty(propertyName, BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance);
                var value = isObj ? (prop != null ? prop.GetValue(key) : keyType.GetField(propertyName, BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance).GetValue(key)) : key;
                dict.Add(propertyName, value);
            }
            return dict;
        }

        private T GetKeyObject<T>(Dictionary<string, object> dict)
        {
            Type type = typeof(T);
            var obj = Activator.CreateInstance(type);

            foreach (var kv in dict)
            {
                type.GetProperty(kv.Key).SetValue(obj, kv.Value);
            }
            return (T)obj;
        }

        private string[] GetPartitionKeys()
        {
            var mapping = MappingConfiguration.Global.Get<TEntity>();
            if (mapping?.PartitionKeys.Any() == true)
            {
                return mapping.PartitionKeys;
            }

            var itemType = typeof(TEntity);
            return itemType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.GetCustomAttributes(typeof(PartitionKeyAttribute), false).Any() ||
                            p.GetCustomAttributes(typeof(PrimaryKeyAttribute), false).Any())
                .Select(p => p.Name).ToArray();
        }
    }
}