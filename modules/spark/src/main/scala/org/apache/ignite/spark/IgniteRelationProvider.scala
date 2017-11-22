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

package org.apache.ignite.spark

import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.internal.IgnitionEx
import org.apache.ignite.internal.util.IgniteUtils
import org.apache.ignite.spark.IgniteRelationProvider._
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder
import org.apache.ignite.{IgniteException, Ignition}
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._

import scala.collection.JavaConversions._

import org.apache.spark.sql.ignite._

/**
  * Apache Ignite relation provider.
  */
class IgniteRelationProvider extends RelationProvider with DataSourceRegister {
    /**
      * @return "ignite" - name of relation provider.
      */
    override def shortName(): String = IGNITE

    /**
      * To create IgniteRelation we need a link to a ignite cluster and a table name.
      * To refer cluster user have to specify one of config parameter:
      * <ul>
      *     <li><code>config</code> - path to ignite configuration file.
      *     <li><code>grid</code> - grid name. Note that grid has to be started in the same jvm.
      * <ul>
      * Existing table inside Apache Ignite should be referred via <code>table</code> parameter.
      *
      * @param sqlCtx SQLContext.
      * @param params Parameters for relation creation.
      * @return IgniteRelation.
      * @see IgniteRelation
      * @see IgnitionEx#grid(String)
      */
    override def createRelation(sqlCtx: SQLContext, params: Map[String, String]): BaseRelation = {
        val igniteHome = IgniteUtils.getIgniteHome

        def configProvider: () ⇒ IgniteConfiguration = {
            if (params.contains(TCP_IP_ADDRESSES))
                () ⇒ {
                    val cfg = new IgniteConfiguration

                    val discoverySpi = new TcpDiscoverySpi

                    val ipFinder = new TcpDiscoveryMulticastIpFinder

                    ipFinder.setAddresses(params(TCP_IP_ADDRESSES).split(";").toList)

                    discoverySpi.setIpFinder(ipFinder)

                    cfg.setDiscoverySpi(discoverySpi)

                    cfg.setClientMode(true)

                    if (params.contains(PEER_CLASS_LOADING))
                        cfg.setPeerClassLoadingEnabled(params(PEER_CLASS_LOADING).toBoolean)

                    cfg
                }
            else if (params.contains(CONFIG_FILE))
                () ⇒ {
                    IgniteContext.setIgniteHome(igniteHome)

                    val cfg = IgnitionEx.loadConfiguration(params(CONFIG_FILE)).get1()

                    cfg.setClientMode(true)

                    if (params.contains(PEER_CLASS_LOADING))
                        cfg.setPeerClassLoadingEnabled(params(PEER_CLASS_LOADING).toBoolean)

                    cfg
                }
            else if (params.contains(GRID))
                () ⇒ {
                    IgniteContext.setIgniteHome(igniteHome)

                    val cfg = ignite(params(GRID)).configuration()

                    cfg.setClientMode(true)

                    if (params.contains(PEER_CLASS_LOADING))
                        cfg.setPeerClassLoadingEnabled(params(PEER_CLASS_LOADING).toBoolean)

                    cfg
                }
            else
                throw new IgniteException("'config' or 'grid' or 'tcpIpAddresses' must be specified to connect to ignite cluster.")
        }

        val cfg = configProvider()

        sqlCtx.sparkContext.addSparkListener(new SparkListener {
            override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
                Ignition.stop(cfg.getIgniteInstanceName, true)
            }
        })

        val ic = IgniteContext(sqlCtx.sparkContext, configProvider, standalone = false)

        if (params.contains(TABLE))
            IgniteSQLRelation(ic, params(TABLE).toUpperCase, sqlCtx)
        else if (params.contains(CACHE)) {
            val cache = params(CACHE)

            if (!params.contains(KEY_CLASS) || !params.contains(VALUE_CLASS))
                throw new IgniteException("'keyClass' and 'valueClass' must be specified for a 'cache'")

            val keepBinary = params.getOrElse(KEEP_BINARY, "true").toBoolean

            IgniteCacheRelation(ic, cache, Class.forName(params(KEY_CLASS)),
                Class.forName(params(VALUE_CLASS)), keepBinary, sqlCtx)
        }
        else
            throw new IgniteException("'table' or 'cache' must be specified for loading ignite data.")
    }
}

object IgniteRelationProvider {
    /**
      * Name of DataSource format for loading data from Apache Ignite.
      */
    val IGNITE = "ignite"

    /**
      * Config option to specify named grid instance to connect when loading data.
      *
      * @example {{{
      * val igniteDF = spark.read.format(IGNITE)
      *     .option(GRID, "my-grid")
      *     //.... other options ...
      *     .load()
      * }}}
      *
      * @see [[org.apache.ignite.Ignite#name()]]
      */
    val GRID = "grid"


    /**
      * Config option to specify path to ignite config file.
      * Config from this file will be used to connect to existing Ignite cluster.
      *
      * @note All nodes for executing Spark task forcibly will be started in client mode.
      *
      * @example {{{
      * val igniteDF = spark.read.format(IGNITE)
      *     .option(CONFIG_FILE, CONFIG_FILE)
      *     // other options ...
      *     .load()
      * }}}
      */
    val CONFIG_FILE = "config"

    /**
      * Config option to provide semi colon separated list of addresses to discover IgniteCluster.
      * `TcpDiscoverySpi` and `TcpDiscoveryMulticastIpFinder` will be used for discovering.
      *
      * @example {{{
      * val igniteDF = spark.read.format(IGNITE)
      *     .option(TCP_IP_ADDRESSES, "172.17.0.1:47500..47509;127.17.0.2:47500..47509")
      *     // other options ...
      *     .load()
      * }}}
      *
      * @see [[org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi]]
      * @see [[org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi#setIpFinder]]
      * @see [[org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder]]
      * @see [[org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder#setAddresses]]
      */
    val TCP_IP_ADDRESSES = "tcpIpAddresses"

    /**
      * Config option to specify Ignite SQL table name to load data from.
      *
      * @example {{{
      * val igniteDF = spark.read.format(IGNITE)
      *     // other options ...
      *     .option(TABLE, "mytable")
      *     .load()
      * }}}
      *
      * @see [[org.apache.ignite.cache.QueryEntity#tableName]]
      */
    val TABLE = "table"

    /**
      * Config option to specify Ignite cache name to load data from.
      *
      * @example {{{
      * val df = spark.read.format(IGNITE)
      *     // other options ...
      *     .option(CACHE, "mycache")
      *     .option(KEY_CLASS, "java.lang.Long")
      *     .option(VALUE_CLASS, "java.lang.String")
      *     .load()
      * }}}
      *
      * @see [[javax.cache.Cache#getName()]]
      */
    val CACHE = "cache"

    /**
      * Config option to specify java class for Ignite cache keys.
      *
      * @example {{{
      * val df = spark.read.format(IGNITE)
      *     //other options ...
      *     .option(CACHE, "mycache")
      *     .option(KEY_CLASS, "java.lang.Long")
      *     .load()
      * }}}
      *
      */
    val KEY_CLASS = "keyClass"

    /**
      * Config option to specify java class for Ignite cache values.
      *
      * @example {{{
      * val df = spark.read.format(IGNITE)
      *     // other options ...
      *     .option(CACHE, "mycache")
      *     .option(VALUE_CLASS, "java.lang.Long")
      *     .load()
      * }}}
      *
      */
    val VALUE_CLASS = "valueClass"

    /**
      * Config option to specify usage of Ignite BinaryMarshaller.
      * Can be used only in combination with `CACHE` as long as all SQL table in Ignite already has `keepBinary=true`.
      * Default value is `true`.
      *
      * @see [[org.apache.ignite.IgniteCache#withKeepBinary()]]
      * @see [[org.apache.ignite.internal.binary.BinaryMarshaller]]
      * @see [[org.apache.ignite.spark.IgniteRelationProvider#CACHE]]
      */
    val KEEP_BINARY = "keepBinary"

    /**
      * Config option to setup peer class loading for Ignite client node started for task execution.
      *
      * @see [[org.apache.ignite.configuration.IgniteConfiguration#setPeerClassLoadingEnabled]]
      */
    val PEER_CLASS_LOADING = "peerClassLoading"
}
