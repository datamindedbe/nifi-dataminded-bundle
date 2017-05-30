/*
 * Copyright 2017 Data Minded
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package be.dataminded.nifi.plugins;


import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.nifi.attribute.expression.language.StandardExpressionLanguageCompiler;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageCompiler;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.registry.VariableDescriptor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockVariableRegistry;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.file.FileUtils;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static be.dataminded.nifi.plugins.GenerateOracleTableFetch.REL_SUCCESS;
import static org.assertj.core.api.Assertions.assertThat;


/**
 * Unit tests for the GenerateTableFetch processor.
 */
public class TestGenerateOracleTableFetch {

    private final static String DB_LOCATION = "target/db_gtf";

    private TestRunner runner;

    @BeforeClass
    public static void setupBeforeClass() throws IOException {
        System.setProperty("derby.stream.error.file", "target/derby.log");

        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        try {
            FileUtils.deleteFile(dbLocation, true);
        } catch (IOException ioe) {
            // Do nothing, may not have existed
        }
    }

    @AfterClass
    public static void cleanUpAfterClass() throws Exception {
        try {
            DriverManager.getConnection("jdbc:derby:" + DB_LOCATION + ";shutdown=true");
        } catch (SQLNonTransientConnectionException e) {
            // Do nothing, this is what happens at Derby shutdown
        }
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        try {
            FileUtils.deleteFile(dbLocation, true);
        } catch (IOException ioe) {
            // Do nothing, may not have existed
        }
    }

    @Before
    public void setUp() throws Exception {
        final DBCPService dbcp = new DBCPServiceSimpleImpl();
        final Map<String, String> dbcpProperties = new HashMap<>();

        runner = TestRunners.newTestRunner(GenerateOracleTableFetch.class);
        runner.addControllerService("dbcp", dbcp, dbcpProperties);
        runner.enableControllerService(dbcp);
        runner.setProperty(GenerateOracleTableFetch.DBCP_SERVICE, "dbcp");
    }


    private void dropTestTable(Connection con) {
        try (Statement statement = con.createStatement()) {
            statement.execute("DROP TABLE TEST_QUERY_DB_TABLE");
        } catch (SQLException ignored) {
        }
    }


    @Test
    @Ignore("derby database does not have oracle mode. Suggestion: look at H2")
    public void testGenerateFullTableLoad() throws ClassNotFoundException, SQLException, InitializationException, IOException {
        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        dropTestTable(con);
        stmt.execute("CREATE TABLE TEST_QUERY_DB_TABLE (id INTEGER NOT NULL, name VARCHAR(100))");
        stmt.execute("INSERT INTO TEST_QUERY_DB_TABLE (id, name) VALUES (1, 'Joe Smith')");
        stmt.execute("INSERT INTO TEST_QUERY_DB_TABLE (id, name) VALUES (25, 'John Doe')");
        stmt.execute("INSERT INTO TEST_QUERY_DB_TABLE (id, name) VALUES (50, 'Joey Johnson')");
        stmt.execute("INSERT INTO TEST_QUERY_DB_TABLE (id, name) VALUES (75, 'Jasper Smith')");
        stmt.execute("INSERT INTO TEST_QUERY_DB_TABLE (id, name) VALUES (100, 'Jason Statham')");


        int numberOfPartitions = 4;

        runner.setProperty(GenerateOracleTableFetch.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(GenerateOracleTableFetch.SPLIT_COLUMN, "ID");
        runner.setProperty(GenerateOracleTableFetch.TENANT, "TENANT");
        runner.setProperty(GenerateOracleTableFetch.SCHEMA, "APP");
        runner.setProperty(GenerateOracleTableFetch.SOURCE, "SOURCE");
        runner.setProperty(GenerateOracleTableFetch.NUMBER_OF_PARTITIONS, String.valueOf(numberOfPartitions));
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS, numberOfPartitions);

        runner.assertAllFlowFilesContainAttribute("table.name");
        runner.assertAllFlowFilesContainAttribute("tenant.name");
        runner.assertAllFlowFilesContainAttribute("schema.name");
        runner.assertAllFlowFilesContainAttribute("source.name");

        List<String> queries = Lists.newArrayList();
        for (MockFlowFile flowFile : runner.getFlowFilesForRelationship(REL_SUCCESS)) {
            queries.add(new String(flowFile.toByteArray()));
        }

        assertThat(queries).contains(
                "SELECT * FROM APP.TEST_QUERY_DB_TABLE WHERE ID BETWEEN 1 AND 24",
                "SELECT * FROM APP.TEST_QUERY_DB_TABLE WHERE ID BETWEEN 25 AND 48",
                "SELECT * FROM APP.TEST_QUERY_DB_TABLE WHERE ID BETWEEN 49 AND 72",
                "SELECT * FROM APP.TEST_QUERY_DB_TABLE WHERE ID BETWEEN 73 AND 100");


        List<String> names = queries.stream().flatMap(query -> {
                                                          try {
                                                              List<String> results = Lists.newArrayList();
                                                              ResultSet resultSet = stmt.executeQuery(query.replace("ID.", ""));
                                                              while (resultSet.next()) {
                                                                  results.add(resultSet.getString("name"));
                                                              }
                                                              return results.stream();
                                                          } catch (SQLException ignore) {
                                                          }
                                                          return new ArrayList<String>().stream();
                                                      }
        ).collect(Collectors.toList());
        assertThat(names).containsExactly("Joe Smith", "John Doe", "Joey Johnson", "Jasper Smith", "Jason Statham");

    }


    @Test
    @Ignore
    public void testGenerateIncrementalTableLoad() throws ClassNotFoundException, SQLException, InitializationException, IOException {
        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        dropTestTable(con);
        stmt.execute("CREATE TABLE TEST_QUERY_DB_TABLE (id INTEGER NOT NULL, name VARCHAR(100))");
        stmt.execute("INSERT INTO TEST_QUERY_DB_TABLE (id, name) VALUES (1, 'Joe Smith')");
        stmt.execute("INSERT INTO TEST_QUERY_DB_TABLE (id, name) VALUES (25, 'John Doe')");
        stmt.execute("INSERT INTO TEST_QUERY_DB_TABLE (id, name) VALUES (50, 'Joey Johnson')");
        stmt.execute("INSERT INTO TEST_QUERY_DB_TABLE (id, name) VALUES (75, 'Jasper Smith')");
        stmt.execute("INSERT INTO TEST_QUERY_DB_TABLE (id, name) VALUES (100, 'Jason Statham')");

        int numberOfPartitions = 2;

        runner.setProperty(GenerateOracleTableFetch.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(GenerateOracleTableFetch.SPLIT_COLUMN, "ID");
        runner.setProperty(GenerateOracleTableFetch.TENANT, "TENANT");
        runner.setProperty(GenerateOracleTableFetch.SCHEMA, "APP");
        runner.setProperty(GenerateOracleTableFetch.SOURCE, "SOURCE");
        runner.setProperty(GenerateOracleTableFetch.NUMBER_OF_PARTITIONS, String.valueOf(numberOfPartitions));
        runner.setProperty(GenerateOracleTableFetch.MAX_VALUE_COLUMN, "ID");
        runner.setProperty(GenerateOracleTableFetch.MAX_VALUE_COLUMN_TYPE, GenerateOracleTableFetch.MAX_VALUE_COLUMN_TYPE_INT);
        runner.setProperty(GenerateOracleTableFetch.MAX_VALUE_COLUMN_START_VALUE, String.valueOf(49));
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS, numberOfPartitions);

        runner.assertAllFlowFilesContainAttribute("table.name");
        runner.assertAllFlowFilesContainAttribute("tenant.name");
        runner.assertAllFlowFilesContainAttribute("schema.name");
        runner.assertAllFlowFilesContainAttribute("source.name");

        List<String> queries = Lists.newArrayList();
        for (MockFlowFile flowFile : runner.getFlowFilesForRelationship(REL_SUCCESS)) {
            queries.add(new String(flowFile.toByteArray()));
        }

        assertThat(queries).contains(
                "SELECT * FROM APP.TEST_QUERY_DB_TABLE WHERE ID BETWEEN 50 AND 74",
                "SELECT * FROM APP.TEST_QUERY_DB_TABLE WHERE ID BETWEEN 75 AND 100");


        List<String> names = queries.stream().flatMap(query -> {
                    try {
                        List<String> results = Lists.newArrayList();
                        ResultSet resultSet = stmt.executeQuery(query.replace("ID.", ""));
                        while (resultSet.next()) {
                            results.add(resultSet.getString("name"));
                        }
                        return results.stream();
                    } catch (SQLException ignore) {
                    }
                    return new ArrayList<String>().stream();
                }
        ).collect(Collectors.toList());
        assertThat(names).containsExactly("Joe Smith", "John Doe", "Joey Johnson", "Jasper Smith", "Jason Statham");

    }




    /**
     * Simple implementation only for ListDatabaseTables processor testing.
     */
    private class DBCPServiceSimpleImpl extends AbstractControllerService implements DBCPService {

        @Override
        public String getIdentifier() {
            return "dbcp";
        }

        @Override
        public Connection getConnection() throws ProcessException {
            try {
                Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
                return DriverManager.getConnection("jdbc:derby:" + DB_LOCATION + ";create=true");
            } catch (final Exception e) {
                throw new ProcessException("getConnection failed: " + e);
            }
        }
    }
}