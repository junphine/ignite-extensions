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

package org.apache.ignite.cdc.postgresql;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.Set;
import javax.sql.DataSource;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cdc.CdcConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.internal.util.function.ThrowableFunction;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.postgresql.ds.PGSimpleDataSource;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/** */
public abstract class CdcPostgreSqlReplicationAbstractTest extends GridCommonAbstractTest {
    /** */
    protected static final int BATCH_SIZE = 128;

    /** */
    protected static final int KEYS_CNT = 1024;

    /** Embedded Postgres working directory. */
    private static File pgDir;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        // Clean up and set PG working directory in order to overcome possible inconsistent cleanup of '/tmp'.
        pgDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), "embedded-pg", true, false);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        U.delete(pgDir);
    }


    protected DataSource initPostgres(){
        PGSimpleDataSource dataSource = new PGSimpleDataSource();

        String host="127.0.0.1";
        int port = 5432;
        String database = "test_db";
        String username = "postgres";
        String password = "postgres";

        dataSource.setServerNames(new String[]{host});
        dataSource.setPortNumbers(new int[]{port});
        dataSource.setDatabaseName(database);
        dataSource.setUser(username);
        dataSource.setPassword(password);

        // 可选配置
        dataSource.setConnectTimeout(30);      // 连接超时（秒）
        dataSource.setSocketTimeout(60);       // Socket超时（秒）
        dataSource.setSsl(false);              // SSL配置

        return dataSource;
    }


    protected String getJdbcUrl(String user,String pwd){
        return String.format("jdbc:postgresql://127.0.0.1:5432/test_db?user={}&password={}",user,pwd);
    }

    /** */
    protected void executeOnIgnite(IgniteEx src, String sqlText, Object... args) {
        SqlFieldsQuery qry = new SqlFieldsQuery(sqlText).setArgs(args);

        try (FieldsQueryCursor<List<?>> cursor = src.context().query().querySqlFields(qry, true)) {
            cursor.getAll();
        }
    }

    /** */
    protected ResultSet selectOnPostgreSql(DataSource postgres, String qry) {
        try (Connection conn = postgres.getConnection()) {
            PreparedStatement stmt = conn.prepareStatement(qry);

            return stmt.executeQuery();
        }
        catch (SQLException e) {
            throw new IgniteException(e);
        }
    }

    /** */
    protected boolean selectOnPostgreSqlAndAct(
        DataSource postgres,
        String qry,
        ThrowableFunction<Boolean, ResultSet, SQLException> action
    ) {
        try (Connection conn = postgres.getConnection()) {
            PreparedStatement stmt = conn.prepareStatement(qry);

            try (ResultSet rs = stmt.executeQuery()) {
                return action.apply(rs);
            }
        }
        catch (SQLException e) {
            throw new IgniteException(e);
        }
    }

    /** */
    protected void executeOnPostgreSql(DataSource postgres, String qry) {
        try (Connection conn = postgres.getConnection()) {
            PreparedStatement stmt = conn.prepareStatement(qry);

            stmt.executeUpdate();
        }
        catch (SQLException e) {
            throw new IgniteException(e);
        }
    }

    /** */
    protected boolean checkRow(
        DataSource postgres,
        String tableName,
        String columnName,
        String expected,
        String condition
    ) {
        String qry = "SELECT " + columnName + " FROM " + tableName + " WHERE " + condition;

        try (ResultSet res = selectOnPostgreSql(postgres, qry)) {
            if (res.next()) {
                String actual = res.getString(columnName);

                return expected.equals(actual);
            }

            return false;
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }
    }

    /** */
    protected GridAbsPredicate waitForTablesCreatedOnPostgres(DataSource postgres, Set<String> caches) {
        return () -> {
            String sql = "SELECT EXISTS (" +
                "  SELECT 1 FROM information_schema.tables " +
                "  WHERE table_name = '%s'" +
                ")";

            for (String cache : caches) {
                try (ResultSet rs = selectOnPostgreSql(postgres, String.format(sql, cache.toLowerCase()))) {
                    rs.next();

                    if (!rs.getBoolean(1))
                        return false;
                }
                catch (SQLException e) {
                    log.error(e.getMessage(), e);

                    throw new IgniteException(e);
                }
            }

            return true;
        };
    }

    /** */
    protected GridAbsPredicate waitForTableSize(DataSource postgres, String tableName, long expSz) {
        return () -> {
            try (ResultSet res = selectOnPostgreSql(postgres, "SELECT COUNT(*) FROM " + tableName)) {
                res.next();

                long cnt = res.getLong(1);

                return cnt == expSz;
            }
            catch (SQLException e) {
                throw new IgniteException(e);
            }
        };
    }

    /** */
    protected IgniteToPostgreSqlCdcConsumer getCdcConsumerConfiguration() {
        return new IgniteToPostgreSqlCdcConsumer()
            .setBatchSize(BATCH_SIZE)
            .setOnlyPrimary(true)
            .setCreateTables(false);
    }

    /**
     * @param igniteCfg Ignite configuration.
     * @param caches Cache name set to stream to PostgreSql.
     * @param dataSrc Data Source.
     * @return Future for Change Data Capture application.
     */
    protected IgniteInternalFuture<?> startIgniteToPostgreSqlCdcConsumer(
        IgniteConfiguration igniteCfg,
        Set<String> caches,
        DataSource dataSrc
    ) {
        IgniteToPostgreSqlCdcConsumer cdcCnsmr = getCdcConsumerConfiguration()
            .setCaches(caches)
            .setDataSource(dataSrc);

        CdcConfiguration cdcCfg = new CdcConfiguration();

        cdcCfg.setConsumer(cdcCnsmr);

        return runAsync(new CdcMain(igniteCfg, null, cdcCfg), "ignite-src-to-postgres-" + igniteCfg.getConsistentId());
    }
}
