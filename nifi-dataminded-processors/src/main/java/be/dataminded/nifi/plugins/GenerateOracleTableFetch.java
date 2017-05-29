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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;


@Tags({"database", "sql", "table", "dataminded"})
@CapabilityDescription("Generate queries to extract all data from database tables")
@WritesAttributes({
        @WritesAttribute(attribute = "tenant.name", description = "Hint for which tenant this data is ingested"),
        @WritesAttribute(attribute = "source.name", description = "Hint for which source this data is ingested"),
        @WritesAttribute(attribute = "schema.name", description = "Hint for which schema this data is ingested"),
        @WritesAttribute(attribute = "table.name", description = "The table name for which the queries are generated"),
        @WritesAttribute(attribute = "generateoracletablefetch.total.row.count", description = "the count of the complete table"),
        @WritesAttribute(attribute = "fragment.identifier", description = "All split FlowFiles produced from this query will have the same randomly generated UUID added for this attribute"),
        @WritesAttribute(attribute = "fragment.index", description = "A one-up number that indicates the ordering of the split FlowFiles that were created from a single parent FlowFile"),
        @WritesAttribute(attribute = "fragment.count", description = "The number of split FlowFiles generated from the parent FlowFile"),
        @WritesAttribute(attribute = "segment.original.filename ", description = "The filename for all flowFiles (defragment expects this)")
        })
public class GenerateOracleTableFetch extends AbstractProcessor {

    static final Relationship REL_SUCCESS;

    static final PropertyDescriptor DBCP_SERVICE;
    static final PropertyDescriptor TABLE_NAME;
    static final PropertyDescriptor COLUMN_NAMES;
    static final PropertyDescriptor QUERY_TIMEOUT;
    static final PropertyDescriptor NUMBER_OF_PARTITIONS;
    static final PropertyDescriptor SPLIT_COLUMN;

    static final PropertyDescriptor TENANT;
    static final PropertyDescriptor SOURCE;
    static final PropertyDescriptor SCHEMA;

    static final PropertyDescriptor OPTION_TO_NUMBER;

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final ComponentLog logger = getLogger();

        final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        final String tableName = context.getProperty(TABLE_NAME).getValue();
        final String schema = context.getProperty(SCHEMA).getValue();
        final String columnNames = context.getProperty(COLUMN_NAMES).getValue();
        final String splitColumnName = context.getProperty(SPLIT_COLUMN).getValue();
        final int numberOfFetches = Integer.parseInt(context.getProperty(NUMBER_OF_PARTITIONS).getValue());
        final Integer queryTimeout = context.getProperty(QUERY_TIMEOUT).asTimePeriod(TimeUnit.SECONDS).intValue();
        final boolean optionalToNumber = context.getProperty(OPTION_TO_NUMBER).asBoolean();

        try {

            String queryStatement = "SELECT MIN(%s), MAX(%s), COUNT(%s) FROM %s.%s";
            if(optionalToNumber) {queryStatement = "SELECT MIN(TO_NUMBER(%s)), MAX(TO_NUMBER(%s)), COUNT(%s) FROM %s.%s";}

            String selectQuery = String.format(queryStatement,
                    splitColumnName,
                    splitColumnName,
                    splitColumnName,
                    schema,
                    tableName);

            long low, high, numberOfRecords;

            // Fetch metadata from the database //
            try (final Connection con = dbcpService.getConnection();
                 final Statement statement = con.createStatement()) {
                statement.setQueryTimeout(queryTimeout);

                logger.debug("Executing {}", new Object[]{selectQuery});
                ResultSet resultSet = statement.executeQuery(selectQuery);
                if (resultSet.next()) {
                    low = resultSet.getLong(1);
                    high = resultSet.getLong(2);
                    numberOfRecords = resultSet.getLong(3);
                } else {
                    logger.error(
                            "Something is very wrong here, one row (even if count is zero) should have been returned: {}",
                            new Object[]{selectQuery});
                    throw new SQLException("No rows returned from metadata query: " + selectQuery);
                }
            } catch (SQLException e) {
                logger.error("Unable to execute SQL select query {} due to {}", new Object[]{selectQuery, e});
                throw new ProcessException(e);
            }

            long chunks = Math.min(numberOfFetches, numberOfRecords);
            long chunkSize = (high - low) / Math.max(chunks, 1);
            final String fragmentIdentifier = UUID.randomUUID().toString();
            for (int i = 0; i < chunks; i++) {
                long min = (low + i * chunkSize);
                long max = (i == chunks - 1) ? high : Math.min((i + 1) * chunkSize - 1 + low, high);
                String query = String.format("SELECT %s FROM %s.%s WHERE %s BETWEEN %s AND %s",
                                             columnNames,
                                             schema,
                                             tableName,
                                             splitColumnName,
                                             min,
                                             max);
                FlowFile sqlFlowFile = session.create();
                sqlFlowFile = session.write(sqlFlowFile, out -> out.write(query.getBytes()));
                sqlFlowFile = session.putAttribute(sqlFlowFile, "table.name", sanitizeAttribute(tableName));

                sqlFlowFile = session.putAttribute(sqlFlowFile, "generateoracletablefetch.total.row.count", String.valueOf(numberOfRecords));

                sqlFlowFile = session.putAttribute(sqlFlowFile, "fragment.identifier", fragmentIdentifier);
                sqlFlowFile = session.putAttribute(sqlFlowFile, "fragment.index", Integer.toString(i));
                sqlFlowFile = session.putAttribute(sqlFlowFile, "segment.original.filename", fragmentIdentifier);
                sqlFlowFile = session.putAttribute(sqlFlowFile, "fragment.count", Long.toString(chunks));

                String tenant = context.getProperty(TENANT).getValue();
                String source = context.getProperty(SOURCE).getValue();

                if (tenant != null) {
                    sqlFlowFile = session.putAttribute(sqlFlowFile, "tenant.name", sanitizeAttribute(tenant));
                }

                if (source != null) {
                    sqlFlowFile = session.putAttribute(sqlFlowFile, "source.name", sanitizeAttribute(source));

                }

                if (schema != null) {
                    sqlFlowFile = session.putAttribute(sqlFlowFile, "schema.name", sanitizeAttribute(schema));
                }

                session.transfer(sqlFlowFile, REL_SUCCESS);
            }

            session.commit();
        } catch (final ProcessException pe) {
            // Log the cause of the ProcessException if it is available
            Throwable t = (pe.getCause() == null ? pe : pe.getCause());
            logger.error("Error during processing: {}", new Object[]{t.getMessage()}, t);
            session.rollback();
            context.yield();
        }
    }

    private String sanitizeAttribute(String attribute) {
        if (attribute == null) {
            return null;
        }
        return attribute.toLowerCase().replaceAll("_", "-");
    }


    @Override
    public Set<Relationship> getRelationships() {
        return ImmutableSet.of(REL_SUCCESS);
    }


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.of(DBCP_SERVICE,
                                TABLE_NAME,
                                COLUMN_NAMES,
                                QUERY_TIMEOUT,
                                NUMBER_OF_PARTITIONS,
                                SPLIT_COLUMN,
                                TENANT,
                                SOURCE,
                                SCHEMA,
                                OPTION_TO_NUMBER);
    }

    static {
        REL_SUCCESS = new Relationship.Builder()
                .name("success")
                .description("Successfully created FlowFile from SQL query result set.")
                .build();

        DBCP_SERVICE = new org.apache.nifi.components.PropertyDescriptor.Builder()
                .name("Database Connection Pooling Service")
                .description("The Controller Service that is used to obtain a connection to the database.")
                .required(true)
                .identifiesControllerService(DBCPService.class)
                .build();

        TABLE_NAME = new org.apache.nifi.components.PropertyDescriptor.Builder()
                .name("Table Name")
                .description("The name of the database table to be queried.")
                .required(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

        COLUMN_NAMES = new org.apache.nifi.components.PropertyDescriptor.Builder()
                .name("Columns to Return")
                .description(
                        "A comma-separated list of column names to be used in the query. If your database requires special treatment of the names (quoting, e.g.), each name should include such treatment. If no column names are supplied, all columns in the specified table will be returned.")
                .required(false)
                .defaultValue("*")
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build();

        QUERY_TIMEOUT = new org.apache.nifi.components.PropertyDescriptor.Builder()
                .name("Max Wait Time")
                .description(
                        "The maximum amount of time allowed for a running SQL select query , zero means there is no limit. Max time less than 1 second will be equal to zero.")
                .defaultValue("0 seconds")
                .required(true)
                .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
                .build();

        NUMBER_OF_PARTITIONS = new PropertyDescriptor.Builder()
                .name("number-of-partitions")
                .displayName("Number of partitions")
                .defaultValue("4")
                .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
                .build();

        SPLIT_COLUMN = new PropertyDescriptor.Builder()
                .name("split-column")
                .displayName("Split column")
                .required(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
                .build();

        TENANT = new PropertyDescriptor.Builder()
                .name("tenant")
                .displayName("Tenant")
                .required(false)
                .defaultValue(null)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .description("Hint for which tenant this data is ingested")
                .build();

        SOURCE = new PropertyDescriptor.Builder()
                .name("source")
                .displayName("Source")
                .required(false)
                .defaultValue(null)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .description("Hint for which source this data is ingested")
                .build();


        SCHEMA = new PropertyDescriptor.Builder()
                .name("schema")
                .displayName("Schema")
                .defaultValue(null)
                .required(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .description("Hint for which schema this data is ingested")
                .build();

        OPTION_TO_NUMBER = new PropertyDescriptor.Builder()
                .name("optionalToNumber")
                .displayName("Optional to number")
                .defaultValue("false")
                .required(false)
                .allowableValues("true", "false")
                .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
                .description("option if the split column has to be cast to a number")
                .build();
    }
}
