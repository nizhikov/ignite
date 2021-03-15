/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Tests.Services
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Services;
    using NUnit.Framework;
    using Apache.Ignite.Platform.Model;

    /// <summary>
    /// Tests checks ability to execute service method without explicit registration of parameter type.
    /// </summary>
    public class ServicesTypeAutoResolveTest
    {
        /** */
        protected internal static readonly Employee[] Emps = new[]
        {
            new Employee {Fio = "Sarah Connor", Salary = 1},
            new Employee {Fio = "John Connor", Salary = 2}
        };

        /** */
        protected internal static readonly Parameter[] Param = new[]
        {
            new Parameter()
                {Id = 1, Values = new[] {new ParamValue() {Id = 1, Val = 42}, new ParamValue() {Id = 2, Val = 43}}},
            new Parameter()
                {Id = 2, Values = new[] {new ParamValue() {Id = 3, Val = 44}, new ParamValue() {Id = 4, Val = 45}}}
        };

        /** */
        private IIgnite _grid1;

        /** */
        //TODO: add test for nested array fields.
        private IIgnite _client;

        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            StopGrids();
        }

        /// <summary>
        /// Executes before each test.
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            StartGrids();
        }

        /// <summary>
        /// Executes after each test.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            try
            {
                _grid1.GetServices();

                TestUtils.AssertHandleRegistryIsEmpty(1000, _grid1);
            }
            catch (Exception)
            {
                // Restart grids to cleanup
                StopGrids();

                throw;
            }
            finally
            {
                if (TestContext.CurrentContext.Test.Name.StartsWith("TestEventTypes"))
                    StopGrids(); // clean events for other tests
            }
        }

        /// <summary>
        /// Tests Java service invocation.
        /// Types should be resolved implicitly.
        /// </summary>
        [Test]
        public void TestCallPlatformServiceRemote()
        {
            // Deploy .Net service.
            var platformSvcName = nameof(PlatformTestService);

            _grid1.GetServices().DeployClusterSingleton(platformSvcName, new PlatformTestService());

            var svc = _client.GetServices().GetServiceProxy<IJavaService>(platformSvcName);

            try
            {
                DoTestService(svc);
            }
            finally
            {
                _grid1.GetServices().Cancel(platformSvcName);
            }
        }

        /// <summary>
        /// Tests Java service invocation.
        /// Types should be resolved implicitly.
        /// </summary>
        [Test]
        public void TestCallPlatformServiceLocal()
        {
            const string platformSvcName = "PlatformTestService";

            _grid1.GetServices().DeployClusterSingleton(platformSvcName, new PlatformTestService());

            var svc = _grid1.GetServices().GetServiceProxy<IJavaService>(platformSvcName);

            DoTestService(svc);

            _grid1.GetServices().Cancel(platformSvcName);
        }

        /// <summary>
        /// Tests Java service invocation with dynamic proxy.
        /// Types should be resolved implicitly.
        /// </summary>
        [Test]
        public void TestCallJavaServiceDynamicProxy()
        {
            // Deploy Java service
            var javaSvcName = TestUtils.DeployJavaService(_grid1);
            var svc = _grid1.GetServices().GetDynamicServiceProxy(javaSvcName, true);

            DoTestService(new JavaServiceDynamicProxy(svc));
        }

        /// <summary>
        /// Tests Java service invocation on local.
        /// Types should be resolved implicitly.
        /// </summary>
        [Test]
        public void TestCallJavaServiceLocal()
        {
            // Deploy Java service
            var javaSvcName = TestUtils.DeployJavaService(_grid1);

            var svc = _grid1.GetServices().GetServiceProxy<IJavaService>(javaSvcName, false);

            DoTestService(svc);

            _grid1.GetServices().Cancel(javaSvcName);
        }

        /// <summary>
        /// Tests Java service invocation on remote node..
        /// Types should be resolved implicitly.
        /// </summary>
        [Test]
        public void TestCallJavaServiceRemote()
        {
            // Deploy Java service
            var javaSvcName = TestUtils.DeployJavaService(_grid1);

            var svc = _client.GetServices().GetServiceProxy<IJavaService>(javaSvcName, false);

            DoTestService(svc);

            _grid1.GetServices().Cancel(javaSvcName);
        }

        /// <summary>
        /// Tests java service instance.
        /// </summary>
        private static void DoTestService(IJavaService svc)
        {
            Assert.IsNull(svc.testDepartments(null));

            var deps = svc.testDepartments(new[]
            {
                new Department {Name = "HR"},
                new Department {Name = "IT"}
            }.ToList());

            Assert.NotNull(deps);
            Assert.AreEqual(1, deps.Count);
            Assert.AreEqual("Executive", deps.OfType<Department>().ToArray()[0].Name);

            Assert.IsNull(svc.testAddress(null));

            Address addr = svc.testAddress(new Address {Zip = "000", Addr = "Moscow"});

            Assert.AreEqual("127000", addr.Zip);
            Assert.AreEqual("Moscow Akademika Koroleva 12", addr.Addr);

            Assert.AreEqual(42, svc.testOverload(2, Emps));
            Assert.AreEqual(43, svc.testOverload(2, Param));
            Assert.AreEqual(3, svc.testOverload(1, 2));
            Assert.AreEqual(5, svc.testOverload(3, 2));

            Assert.IsNull(svc.testEmployees(null));

            var emps = svc.testEmployees(Emps);

            Assert.AreEqual(typeof(Employee[]), emps.GetType());
            Assert.NotNull(emps);
            Assert.AreEqual(1, emps.Length);

            Assert.AreEqual("Kyle Reese", emps[0].Fio);
            Assert.AreEqual(3, emps[0].Salary);

            Assert.IsNull(svc.testMap(null));

            var map = new Dictionary<Key, Value>();

            map.Add(new Key() {Id = 1}, new Value() {Val = "value1"});
            map.Add(new Key() {Id = 2}, new Value() {Val = "value2"});

            var res = svc.testMap(map);

            Assert.NotNull(res);
            Assert.AreEqual(1, res.Count);
            Assert.AreEqual("value3", ((Value)res[new Key() {Id = 3}]).Val);

            var accs = svc.testAccounts();

            Assert.AreEqual(typeof(Account[]), accs.GetType());
            Assert.NotNull(accs);
            Assert.AreEqual(2, accs.Length);
            Assert.AreEqual("123", accs[0].Id);
            Assert.AreEqual("321", accs[1].Id);
            Assert.AreEqual(42, accs[0].Amount);
            Assert.AreEqual(0, accs[1].Amount);

            var users = svc.testUsers();

            Assert.AreEqual(typeof(User[]), users.GetType());
            Assert.NotNull(users);
            Assert.AreEqual(2, users.Length);
            Assert.AreEqual(1, users[0].Id);
            Assert.AreEqual(ACL.Allow, users[0].Acl);
            Assert.AreEqual("admin", users[0].Role.Name);
            Assert.AreEqual(2, users[1].Id);
            Assert.AreEqual(ACL.Deny, users[1].Acl);
            Assert.AreEqual("user", users[1].Role.Name);
        }

        /// <summary>
        /// Starts the grids.
        /// </summary>
        private void StartGrids()
        {
            if (_grid1 != null)
                return;

            var path = Path.Combine("Config", "Compute", "compute-grid");

            var cfg = GetConfiguration(path + "1.xml");

            _grid1 = Ignition.Start(cfg);

            cfg.ClientMode = true;
            cfg.IgniteInstanceName = "client";
            cfg.WorkDirectory = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location),
                "client_work");

            _client = Ignition.Start(cfg);
        }

        /// <summary>
        /// Stops the grids.
        /// </summary>
        private void StopGrids()
        {
            _grid1 = null;

            Ignition.StopAll(true);
        }

        /// <summary>
        /// Gets the Ignite configuration.
        /// </summary>
        private IgniteConfiguration GetConfiguration(string springConfigUrl)
        {
            springConfigUrl = Compute.ComputeApiTestFullFooter.ReplaceFooterSetting(springConfigUrl);

            return new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SpringConfigUrl = springConfigUrl,
                BinaryConfiguration = new BinaryConfiguration
                {
                    NameMapper = new BinaryBasicNameMapper {NamespacePrefix = "org.", NamespaceToLower = true}
                }
            };
        }

        [Test]
        public void ServiceReturnsArray()
        {
            _client.GetServices().DeployClusterSingleton(nameof(ArrayFactoryService), new ArrayFactoryService());

            try
            {
                var svc = _client.GetServices().GetServiceProxy<IArrayFactory>(nameof(ArrayFactoryService), false);
                var arr = svc.CreateArray(2, 1);

                Assert.AreEqual(typeof(R[]), arr.GetType());
                Assert.AreEqual(1, arr[1].Value);
            }
            finally
            {
                _client.GetServices().Cancel(nameof(ArrayFactoryService));
            }
        }

        [Test]
        public void ServiceReturnsArrayWithReflection()
        {
            _client.GetServices().DeployClusterSingleton(nameof(ArrayFactoryService), new ArrayFactoryService());

            try
            {
                var svc = _client.GetServices().GetServiceProxy<IArrayFactory>(nameof(ArrayFactoryService));

                var mthd = typeof(IArrayFactory).GetMethod(nameof(IArrayFactory.CreateArray));

                var arr = mthd.Invoke(svc, new object[] {2, 1});

                Assert.AreEqual(typeof(R[]), arr.GetType());
                Assert.AreEqual(1, ((R[]) arr)[1].Value);
            }
            finally
            {
                _client.GetServices().Cancel(nameof(ArrayFactoryService));
            }
        }

        public interface IArrayFactory
        {
            R[] CreateArray(int size, int dlftVal);
        }

        public sealed class ArrayFactoryService : IArrayFactory, IService
        {
            public R[] CreateArray(int size, int dfltVal)
            {
                return Enumerable.Repeat(new R { Value = dfltVal }, size).ToArray();
            }

            public void Cancel(IServiceContext context)
            {
            }

            public void Execute(IServiceContext context)
            {
            }

            public void Init(IServiceContext context)
            {
            }
        }
    }
}
