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

import java.lang.{Long ⇒ JLong}

import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.internal.IgnitionEx
import org.apache.ignite.spark.AbstractDataFrameSpec.{INT_STR_CACHE_NAME, TEST_CONFIG_FILE}
import org.apache.spark.sql.ignite.IgniteSparkSession
import org.apache.spark.sql.types.{LongType, StringType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.apache.ignite.spark.impl._

import scala.collection.JavaConversions._

/**
  * Tests to check Spark Catalog implementation.
  */
@RunWith(classOf[JUnitRunner])
class IgniteCatalogSpec extends AbstractDataFrameSpec {
    var igniteSession: IgniteSparkSession = _

    describe("Ignite Catalog Implementation") {
        it("Should observe all available SQL tables") {
            val tables = igniteSession.catalog.listTables.collect()

            tables.length should equal (2)

            tables.map(_.name).sorted should equal (Array("CITY", "PERSON"))
        }

        it("Should provide correct schema for SQL table") {
            val columns = igniteSession.catalog.listColumns("city").collect()

            columns.length should equal (2)

            columns.map(c ⇒ (c.name, c.dataType, c.nullable)).sorted should equal (
                Array(
                    ("ID", LongType.catalogString, false),
                    ("NAME", StringType.catalogString, true)))
        }

        it("Should provide ability to query SQL table without explicit registration") {
            val res = igniteSession.sql("SELECT id, name FROM city").rdd

            res.count should equal(3)

            val cities = res.collect

            cities.map(c ⇒ (c.getAs[JLong]("id"), c.getAs[String]("name"))) should equal (
                Array(
                    (1, "Forest Hill"),
                    (2, "Denver"),
                    (3, "St. Petersburg")
                )
            )
        }

        it("Should provide newly created tables in tables list") {
            val cache = client.cache(INT_STR_CACHE_NAME)

            cache.query(new SqlFieldsQuery(
                "CREATE TABLE new_table(id LONG PRIMARY KEY, name VARCHAR) WITH \"template=replicated\"")).getAll

            val tables = igniteSession.catalog.listTables.collect()

            tables.find(_.name == "NEW_TABLE").map(_.name) should equal (Some("NEW_TABLE"))

            val columns = igniteSession.catalog.listColumns("NEW_TABLE").collect()

            columns.map(c ⇒ (c.name, c.dataType, c.nullable)).sorted should equal (
                Array(
                    ("ID", LongType.catalogString, false),
                    ("NAME", StringType.catalogString, true)))
        }

        it("Should allow register tables based on other datasources") {
            val citiesDataFrame = igniteSession.read.json("src/test/resources/cities.json")

            citiesDataFrame.createOrReplaceTempView("JSON_CITIES")

            val res = igniteSession.sql("SELECT id, name FROM json_cities").rdd

            res.count should equal(3)

            val cities = res.collect

            cities.map(c ⇒ (c.getAs[JLong]("id"), c.getAs[String]("name"))) should equal (
                Array(
                    (1, "Forest Hill"),
                    (2, "Denver"),
                    (3, "St. Petersburg")
                )
            )
        }
    }

    override protected def beforeAll(): Unit = {
        super.beforeAll()

        createPersonTable(client, INT_STR_CACHE_NAME)

        createCityTable(client, INT_STR_CACHE_NAME)

        val configProvider = enclose(null) (x ⇒ () ⇒ {
            val cfg = IgnitionEx.loadConfiguration(TEST_CONFIG_FILE).get1()

            cfg.setClientMode(true)

            cfg.setIgniteInstanceName("client-2")

            cfg
        })

        igniteSession = IgniteSparkSession.builder()
            .config(spark.sparkContext.getConf)
            .igniteConfigProvider(configProvider)
            .getOrCreate()
    }
}
