#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.


"""
This module contains consistency check/repair tests.
"""

from ducktape.errors import TimeoutError
from ducktape.mark import defaults

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.ignite_execution_exception import IgniteExecutionException
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.services.utils.ignite_configuration.event_type import EventType
from ignitetest.tests.util import preload_data, DataGenerationParams, current_millis
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion


class ConsistencyTest(IgniteTest):
    """
    Consistency test.
    """
    CACHE_NAME = "TEST"

    @cluster(num_nodes=2)
    @ignite_versions(str(DEV_BRANCH))
    def test_logging(self, ignite_version):
        """
        Tests logging goes to the correct file (consistency.log) when default AI config used.
        """
        cfg_filename = "ignite-default-log4j2.xml"

        ignites = IgniteApplicationService(
            self.test_context,
            IgniteConfiguration(
                version=IgniteVersion(ignite_version),
                cluster_state="INACTIVE",
                include_event_types=[EventType.EVT_CONSISTENCY_VIOLATION],
                log4j_config=cfg_filename  # default AI config (will be generated below)
            ),
            java_class_name="org.apache.ignite.internal.ducktest.tests.control_utility.InconsistentNodeApplication",
            params={
                "cacheName": self.CACHE_NAME,
                "amount": 1024,
                "parts": 1,
                "tx": False
            },
            startup_timeout_sec=180,
            num_nodes=self.available_cluster_size)

        for node in ignites.nodes:  # copying default AI config with log path replacement
            ignites.init_persistent(node)

            cfg_file = f"{ignites.config_dir}/{cfg_filename}"

            ignites.exec_command(node, f"cp {ignites.home_dir}/config/ignite-log4j.xml {cfg_file}")

            orig = "${sys:IGNITE_HOME}/work/log".replace('/', '\\/')
            fixed = ignites.log_dir.replace('/', '\\/')

            ignites.exec_command(node, f"sed -i 's/{orig}/{fixed}/g' {cfg_file}")

        ignites.start()

        control_utility = ControlUtility(ignites)

        control_utility.activate()

        ignites.await_event("APPLICATION_STREAMING_FINISHED", 60, from_the_beginning=True)

        try:
            control_utility.idle_verify()  # making sure we have broken data
            raise IgniteExecutionException("Fail.")
        except AssertionError:
            pass

        # checking/repairing
        control_utility.check_consistency(f"repair --cache {self.CACHE_NAME} --partition 0 --strategy LWW")

        message = "Cache consistency violations recorded."

        ignites.await_event(message, 60, from_the_beginning=True, log_file="consistency.log")

        try:
            ignites.await_event(message, 10, from_the_beginning=True)
            raise IgniteExecutionException("Fail.")
        except TimeoutError:
            pass

    @cluster(num_nodes=4)
    @ignite_versions(str(DEV_BRANCH))
    @defaults(backups=[1], cache_count=[1], entry_count=[50_000], entry_size=[50_000], preloaders=[1])
    def test_consistency_check_performance(self, ignite_version, backups, cache_count, entry_count, entry_size,
                                           preloaders):
        """
        Tests time of performing consistency check and idle_verify utilities over the same data.
        """

        data_gen_params = DataGenerationParams(backups=backups, cache_count=cache_count, entry_count=entry_count,
                                               entry_size=entry_size, preloaders=preloaders)

        node_config = IgniteConfiguration(
            version=IgniteVersion(ignite_version),
            cluster_state="INACTIVE",
            include_event_types=[EventType.EVT_CONSISTENCY_VIOLATION],
            data_storage=DataStorageConfiguration(
                max_wal_archive_size=2 * data_gen_params.data_region_max_size,
                default=DataRegionConfiguration(
                    persistence_enabled=True,
                    max_size=data_gen_params.data_region_max_size
                )
            ),
            metric_exporters={"org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi"}
        )

        ignites = IgniteService(self.test_context,
                                config=node_config,
                                num_nodes=self.test_context.available_cluster_size - data_gen_params.preloaders)
        ignites.start()

        control_utility = ControlUtility(ignites)

        control_utility.activate()

        preload_time = preload_data(
            self.test_context,
            ignites.config._replace(client_mode=True, discovery_spi=from_ignite_cluster(ignites)),
            data_gen_params=data_gen_params)

        start = current_millis()

        control_utility.idle_verify()

        finish = current_millis()

        idle_verify_time = {
            'start': start,
            'finish': finish,
            'time_to_run': (finish - start)
        }

        self.logger.info("Idle verify finished [time_to_run=" + str(idle_verify_time['time_to_run']) + ']')

        start = current_millis()

        for p in range(0, 1024):
            self.logger.debug('Running repair[p=' + str(p) + ']')
            # checking/repairing
            control_utility.check_consistency(f"repair --cache test-cache-1 --strategy LWW --partition " + str(p))

        finish = current_millis()

        check_consistency_time = {
            'start': start,
            'finish': finish,
            'time_to_run': (finish - start)
        }

        self.logger.info("Check consistency finished [time_to_run=" + str(check_consistency_time['time_to_run']) + ']')

        return {
            'idle_verify': idle_verify_time,
            'check_consistency': check_consistency_time,
            'preload': preload_time
        }
