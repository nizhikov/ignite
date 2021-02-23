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

package org.apache.ignite.internal.processors.query;

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_TO_STRING_INCLUDE_SENSITIVE;

/**
 * Tests check that with {@link IgniteSystemProperties#IGNITE_TO_STRING_INCLUDE_SENSITIVE} == false literals from query
 * will be deleted from query before logging it to history, events, profiling tool.
 */
public class HideLiteralsFromQueryTest extends AbstractIndexingCommonTest {
    @Test
    @WithSystemProperty(key = IGNITE_TO_STRING_INCLUDE_SENSITIVE, value = "false")
    public void testIoStatisticsViews() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        List<IgniteBiTuple<String, String>> qries = Arrays.asList(
            F.t("CREATE TABLE TST(id INTEGER PRIMARY KEY, name VARCHAR, age integer)", null),

            F.t("INSERT INTO TST(id, name, age) VALUES(1, 'John Connor', 16)",
                "INSERT INTO PUBLIC.TST( ID, NAME, AGE ) VALUES (?, ?, ?)"),

            F.t("UPDATE TST SET name = 'Sarah Connor' WHERE id = 1",
                "UPDATE PUBLIC.TST SET NAME = ? WHERE ID = ?"),

            F.t("DELETE FROM TST WHERE name = 'Sarah Connor'",
                "DELETE FROM PUBLIC.TST WHERE NAME = ?"),

            F.t("SELECT * FROM TST WHERE name = 'Sarah Connor'",
                "SELECT __Z0.ID, __Z0.NAME, __Z0.AGE FROM PUBLIC.TST __Z0 WHERE __Z0.NAME = ?"),

            F.t("SELECT * FROM TST WHERE name = SUBSTR('Sarah Connor', 0, 2)",
                "SELECT __Z0.ID, __Z0.NAME, __Z0.AGE FROM PUBLIC.TST __Z0 WHERE __Z0.NAME = ?"),

            F.t("SELECT * FROM TST GROUP BY id HAVING name = 'XXX'",
                "SELECT __Z0.ID, __Z0.NAME, __Z0.AGE FROM PUBLIC.TST __Z0 GROUP BY __Z0.ID HAVING __Z0.NAME = ?"),

            F.t("SELECT CONCAT(name, 'xxx') FROM TST",
                "SELECT CONCAT(__Z0.NAME, ?) FROM PUBLIC.TST __Z0"),

            F.t("ALTER TABLE TST ADD COLUMN department VARCHAR(200)", null),

            F.t("DROP TABLE TST", null),

            F.t("KILL SERVICE 'my_service'", null)
        );

        for (IgniteBiTuple<String, String> qry : qries) {
            execSql(ignite, qry.get1());

            String expHist = qry.get2() == null ? qry.get1() : qry.get2();

            List<List<?>> hist = execSql(ignite, "SELECT sql FROM SYS.SQL_QUERIES_HISTORY WHERE sql = ?", expHist);

            assertNotNull(hist);
            assertEquals(1, hist.size());
            assertEquals(hist.get(0).get(0), expHist);

            //TODO: check events.
        }
    }

    /**
     * @param ignite Ignite.
     * @param sql Sql.
     * @param args Args.
     */
    @SuppressWarnings("unchecked")
    private List<List<?>> execSql(Ignite ignite, String sql, Object... args) {
        IgniteCache cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        SqlFieldsQuery qry = new SqlFieldsQuery(sql);

        if (args != null && args.length > 0)
            qry.setArgs(args);

        return cache.query(qry).getAll();
    }
}
