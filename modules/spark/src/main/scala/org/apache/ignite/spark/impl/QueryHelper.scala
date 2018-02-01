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

package org.apache.ignite.spark.impl

import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.spark.IgniteDataFrameSettings._
import QueryUtils.{compileCreateTable, compileDropTable, compileInsert}
import org.apache.ignite.internal.IgniteEx
import org.apache.ignite.spark.IgniteContext
import org.apache.ignite.{Ignite, IgniteException}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Helper class for executing DDL queries.
  */
private[apache] object QueryHelper {
    /**
      * Drops provided table.
      *
      * @param tableName Table name.
      * @param ignite Ignite.
      */
    def dropTable(tableName: String, ignite: Ignite): Unit = {
        val qryProcessor = ignite.asInstanceOf[IgniteEx].context().query()

        val qry = compileDropTable(tableName)

        qryProcessor.querySqlFields(new SqlFieldsQuery(qry), true).getAll
    }

    /**
      * Creates table.
      *
      * @param schema Schema.
      * @param tblName Table name.
      * @param primaryKeyFields Primary key fields.
      * @param createTblOpts Ignite specific options.
      * @param ignite Ignite.
      */
    def createTable(schema: StructType, tblName: String, primaryKeyFields: Seq[String], createTblOpts: Option[String],
        ignite: Ignite): Unit = {
        val qryProcessor = ignite.asInstanceOf[IgniteEx].context().query()

        val qry = compileCreateTable(schema, tblName, primaryKeyFields, createTblOpts)

        qryProcessor.querySqlFields(new SqlFieldsQuery(qry), true).getAll
    }

    /**
      * Ensures all options are specified correctly to create table based on provided `schema`.
      *
      * @param schema Schema of new table.
      * @param params Parameters.
      */
    def ensureCreateTableOptions(schema: StructType, params: Map[String, String], ctx: IgniteContext): Unit = {
        if (!params.contains(OPTION_TABLE) && !params.contains("path"))
            throw new IgniteException("'table' must be specified.")

        params.get(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS)
            .map(_.split(','))
            .getOrElse(throw new IgniteException("Can't create table! Primary key fields has to be specified."))
            .map(_.trim)
            .foreach { pkField ⇒
                if (pkField == "")
                    throw new IgniteException("PK field can't be empty.")

                if (!schema.exists(_.name.equalsIgnoreCase(pkField)))
                    throw new IgniteException(s"'$pkField' doesn't exists in DataFrame schema.")

            }
    }

    /**
      * Saves data to the table.
      *
      * @param data Data.
      * @param tblName Table name.
      * @param ctx Ignite context.
      */
    def saveTable(data: DataFrame, tblName: String, ctx: IgniteContext): Unit = {
        val insertQry = compileInsert(tblName, data.schema)

        data.rdd.foreachPartition(iterator => savePartition(iterator, insertQry, tblName, ctx))
    }

    /**
      * Saves partition data to the Ignite table.
      *
      * @param iterator Data iterator.
      * @param insertQry Insert query.
      * @param tblName Table name.
      * @param ctx Ignite context.
      */
    private def savePartition(iterator: Iterator[Row], insertQry: String, tblName: String, ctx: IgniteContext): Unit = {
        val tblInfo = sqlTableInfo[Any, Any](ctx.ignite(), tblName).get

        val cache = ctx.ignite().cache(tblInfo._1.getName)

        val qry = new SqlFieldsQuery(insertQry)

        iterator.foreach { row ⇒
            val schema = row.schema

            val args = schema.map { f ⇒
                row.get(row.fieldIndex(f.name)).asInstanceOf[Object]
            }

            cache.query(qry.setArgs(args: _*)).getAll
        }
    }
}
