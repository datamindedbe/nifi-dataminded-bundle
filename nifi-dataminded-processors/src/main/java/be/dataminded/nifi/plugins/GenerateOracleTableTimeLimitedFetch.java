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
import be.dataminded.nifi.plugins.util.ArgumentUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;


@Tags({"database", "sql", "table", "dataminded"})
@CapabilityDescription("Generate queries to extract all data from database tables")
@WritesAttribute(attribute = "table.name", description = "The table name for which the queries are generated")
@WritesAttributes({
        @WritesAttribute(attribute = "tenant.name", description = "Hint for which tenant this data is ingested"),
        @WritesAttribute(attribute = "source.name", description = "Hint for which source this data is ingested"),
        @WritesAttribute(attribute = "schema.name", description = "Hint for which schema this data is ingested"),
        @WritesAttribute(attribute = "table.name", description = "The table name for which the queries are generated")
})
public class GenerateOracleTableTimeLimitedFetch extends AbstractProcessor {

    static final Relationship REL_SUCCESS;

    static final PropertyDescriptor TABLE_NAME;
    static final PropertyDescriptor COLUMN_NAMES;
    static final PropertyDescriptor NUMBER_OF_PARTITIONS;
    static final PropertyDescriptor SPLIT_COLUMN;
    static final PropertyDescriptor TIME_FMT;
    static final PropertyDescriptor MIN_BOUND;
    static final PropertyDescriptor MAX_BOUND;

    static final PropertyDescriptor TENANT;
    static final PropertyDescriptor SOURCE;
    static final PropertyDescriptor SCHEMA;

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final ComponentLog logger = getLogger();

        final String tableName = context.getProperty(TABLE_NAME).getValue();
        final String schema = context.getProperty(SCHEMA).getValue();
        final String columnNames = context.getProperty(COLUMN_NAMES).getValue();
        final String splitColumnName = context.getProperty(SPLIT_COLUMN).getValue();
        final String timeFormat = context.getProperty(TIME_FMT).getValue();
        final java.sql.Timestamp minBound = ArgumentUtils.convertStringToTimestamp(
                context.getProperty(MIN_BOUND).getValue(),
                timeFormat);
        final java.sql.Timestamp maxBound = ArgumentUtils.convertStringToTimestamp(
                context.getProperty(MAX_BOUND).getValue(),
                timeFormat);
        final int numberOfFetches = Integer.parseInt(context.getProperty(NUMBER_OF_PARTITIONS).getValue());


        try {
            long chunks = numberOfFetches;
            java.sql.Timestamp low = minBound;
            java.sql.Timestamp high = maxBound;
            long chunkSize = (high.getTime() - low.getTime()) / Math.max(chunks, 1);
            String dateformat = "YYYY-MM-DD HH24:MI:SS.FF3";  // Oracle syntax for ISO8601. Note that a oracle date can store up to seconds, but for milliseconds we need a timestamp.
            for (int i = 0; i < chunks; i++) {
                java.sql.Timestamp min = new java.sql.Timestamp(low.getTime() + i * chunkSize);
                java.sql.Timestamp max = (i == chunks - 1) ? high : new java.sql.Timestamp(Math.min((i + 1) * chunkSize - 1 + low.getTime(), high.getTime()));
                String query = String.format("SELECT %s FROM %s.%s WHERE %s BETWEEN TO_TIMESTAMP('%s', '%s') AND TO_TIMESTAMP('%s', '%s')",
                                             columnNames,
                                             schema,
                                             tableName,
                                             splitColumnName,
                                             min,
                                             dateformat,
                                             max,
                                             dateformat);
                FlowFile sqlFlowFile = session.create();
                sqlFlowFile = session.write(sqlFlowFile, out -> out.write(query.getBytes()));
                sqlFlowFile = session.putAttribute(sqlFlowFile, "table.name", sanitizeAttribute(tableName));

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
        return ImmutableList.of(TABLE_NAME,
                                COLUMN_NAMES,
                                NUMBER_OF_PARTITIONS,
                                SPLIT_COLUMN,
                                TIME_FMT,
                                MIN_BOUND,
                                MAX_BOUND,
                                TENANT,
                                SOURCE,
                                SCHEMA);
    }

    static {
        REL_SUCCESS = new Relationship.Builder()
                .name("success")
                .description("Successfully created FlowFile from SQL query result set.")
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

        NUMBER_OF_PARTITIONS = new org.apache.nifi.components.PropertyDescriptor.Builder()
                .name("number-of-partitions")
                .displayName("Number of partitions")
                .defaultValue("4")
                .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
                .build();

        SPLIT_COLUMN = new org.apache.nifi.components.PropertyDescriptor.Builder()
                .name("split-column")
                .displayName("Split column")
                .required(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
                .build();

        TENANT = new org.apache.nifi.components.PropertyDescriptor.Builder()
                .name("tenant")
                .displayName("Tenant")
                .required(false)
                .defaultValue(null)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .description("Hint for which tenant this data is ingested")
                .build();

        SOURCE = new org.apache.nifi.components.PropertyDescriptor.Builder()
                .name("source")
                .displayName("Source")
                .required(false)
                .defaultValue(null)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .description("Hint for which source this data is ingested")
                .build();


        SCHEMA = new org.apache.nifi.components.PropertyDescriptor.Builder()
                .name("schema")
                .displayName("Schema")
                .defaultValue(null)
                .required(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .description("Hint for which schema this data is ingested")
                .build();

        MIN_BOUND = new org.apache.nifi.components.PropertyDescriptor.Builder()
                .name("Minimum boundary")
                .description("Values in the split-column that are smaller than this threshold will be ignored. "
                             + "If no value is set, the split-column will not be filtered on a lower-bound. ")
                .required(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build();

        MAX_BOUND = new org.apache.nifi.components.PropertyDescriptor.Builder()
                .name("Maximum boundary")
                .description("Values in the split-column that are larger than this threshold will be ignored. "
                             + "If no value is set, the split-column will not be filtered on  a on an upper-bound.")
                .required(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build(); // TODO: look into adding ISO8061_INSTANT_VALIDATOR

        TIME_FMT = new org.apache.nifi.components.PropertyDescriptor.Builder()
                .name("Time format")
                .description("A valid Oracle time format specification, with which the minimum and maximum boundary will be interpreted.")
                .required(false)
                .defaultValue("yyyy-MM-dd")
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build(); // TODO: look into adding ISO8061_INSTANT_VALIDATOR
    }
}
