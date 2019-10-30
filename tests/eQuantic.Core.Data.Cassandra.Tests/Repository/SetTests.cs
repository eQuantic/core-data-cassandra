using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using Cassandra;
using Cassandra.Mapping;
using eQuantic.Core.Data.Cassandra.Repository;
using eQuantic.Core.Data.Cassandra.Tests.Entities;
using FluentAssertions;
using Moq;
using Xunit;

namespace eQuantic.Core.Data.Cassandra.Tests.Repository
{
    [ExcludeFromCodeCoverage]
    public class SetTests
    {
        private Mock<ISession> mockSession;

        public SetTests()
        {
            this.mockSession = new Mock<ISession>();
            var mockCluster = new Mock<ICluster>();
            var mockConfig = new Mock<Configuration>();

            mockCluster.Setup(c => c.Configuration).Returns(mockConfig.Object);
            this.mockSession.Setup(s => s.Cluster).Returns(mockCluster.Object);
            this.mockSession.Setup(s => s.Keyspace).Returns("test");
        }

        [Fact]
        public void Set_GetKeyExpression_WithComplexKey()
        {
            var entities = new List<TestComplexKeyEntity> {
                new TestComplexKeyEntity { Test1 = 1, Test2 = 2 }
            };

            MappingConfiguration.Global.Define(
               new Map<TestComplexKeyEntity>()
                  .TableName("tests")
                  .PartitionKey(u => u.Test1, u => u.Test2)
                  .Column(u => u.Test1, cm => cm.WithName("test1"))
                  .Column(u => u.Test2, cm => cm.WithName("test2")));

            var set = new Set<TestComplexKeyEntity>(mockSession.Object);
            var actualExpression = set.GetKeyExpression(new { Test1 = 1, Test2 = 2 });

            Expression<Func<TestComplexKeyEntity, bool>> expectedExpression = item =>
                (item.Test1 == 1) && (item.Test2 == 2);

            actualExpression.ToString().Should().Be(expectedExpression.ToString());

            var expectedEntity = entities.FirstOrDefault(expectedExpression.Compile());

            var actualEntity = entities.FirstOrDefault(actualExpression.Compile());

            expectedEntity.Should().Be(actualEntity);
        }

        [Fact]
        public void Set_GetKeyExpression_WithPrimitiveKey()
        {
            var entities = new List<TestPrimitiveKeyEntity> {
                new TestPrimitiveKeyEntity { Id = 1 }
            };

            MappingConfiguration.Global.Define(
               new Map<TestPrimitiveKeyEntity>()
                  .TableName("tests")
                  .PartitionKey(u => u.Id)
                  .Column(u => u.Id, cm => cm.WithName("id")));

            var set = new Set<TestPrimitiveKeyEntity>(mockSession.Object);
            var actualExpression = set.GetKeyExpression(1);

            Expression<Func<TestPrimitiveKeyEntity, bool>> expectedExpression = item => (item.Id == 1);

            actualExpression.ToString().Should().Be(expectedExpression.ToString());

            var expectedEntity = entities.FirstOrDefault(expectedExpression.Compile());

            var actualEntity = entities.FirstOrDefault(actualExpression.Compile());

            expectedEntity.Should().Be(actualEntity);
        }
    }
}