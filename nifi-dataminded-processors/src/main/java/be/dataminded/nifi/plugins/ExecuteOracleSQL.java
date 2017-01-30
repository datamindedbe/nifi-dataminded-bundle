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
import com.google.common.collect.Sets;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
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
import org.apache.nifi.util.StopWatch;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@EventDriven
@InputRequirement(Requirement.INPUT_ALLOWED)
@Tags({"sql", "select", "jdbc", "query", "database", "dataminded"})
@CapabilityDescription("Execute provided SQL select query. Query result will be converted to Avro format."
        + " Streaming is used so arbitrarily large result sets are supported. This processor can be scheduled to run on "
        + "a timer, or cron expression, using the standard scheduling methods, or it can be triggered by an incoming FlowFile. "
        + "If it is triggered by an incoming FlowFile, then attributes of that FlowFile will be available when evaluating the "
        + "select query. FlowFile attribute 'executesql.row.count' indicates how many rows were selected.")
@WritesAttribute(attribute = "executesql.row.count", description = "Contains the number of rows returned in the select query")
public class ExecuteOracleSQL extends AbstractProcessor {

    static final String RESULT_ROW_COUNT = "executesql.row.count";

    // Relationships
    static final Relationship SUCCESS;
    static final Relationship FAILURE;

    // Properties
    static final PropertyDescriptor DBCP_SERVICE;
    static final PropertyDescriptor SQL_SELECT_QUERY;
    static final PropertyDescriptor QUERY_TIMEOUT;
    static final PropertyDescriptor NORMALIZE_NAMES_FOR_AVRO;
    static final PropertyDescriptor FETCH_SIZE;

    @OnScheduled
    public void setup(ProcessContext context) {
        // If the query is not set, then an incoming flow file is needed. Otherwise fail the initialization
        if (!context.getProperty(SQL_SELECT_QUERY).isSet() && !context.hasIncomingConnection()) {
            final String errorString = "Either the Select Query must be specified or there must be an incoming " +
                    "connection providing flowfile(s) containing a SQL select query";
            getLogger().error(errorString);
            throw new ProcessException(errorString);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile fileToProcess = null;
        if (context.hasIncomingConnection()) {
            fileToProcess = session.get();

            // If we have no FlowFile, and all incoming connections are self-loops then we can continue on.
            // However, if we have no FlowFile and we have connections coming from other Processors, then
            // we know that we should run only if we have a FlowFile.
            if (fileToProcess == null && context.hasNonLoopConnection()) {
                return;
            }
        }

        final ComponentLog logger = getLogger();
        final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        final Integer queryTimeout = context.getProperty(QUERY_TIMEOUT).asTimePeriod(TimeUnit.SECONDS).intValue();
        final boolean convertNamesForAvro = context.getProperty(NORMALIZE_NAMES_FOR_AVRO).asBoolean();
        final Integer fetchSize = context.getProperty(FETCH_SIZE).asInteger();

        final StopWatch stopWatch = new StopWatch(true);
        final String selectQuery;
        if (context.getProperty(SQL_SELECT_QUERY).isSet()) {
            selectQuery = context.getProperty(SQL_SELECT_QUERY).evaluateAttributeExpressions(fileToProcess).getValue();
        } else {
            // If the query is not set, then an incoming flow file is required, and expected to contain a valid SQL select query.
            // If there is no incoming connection, onTrigger will not be called as the processor will fail when scheduled.
            final StringBuilder queryContents = new StringBuilder();
            session.read(fileToProcess, in -> queryContents.append(IOUtils.toString(in)));
            selectQuery = queryContents.toString();
        }

        try (final Connection con = dbcpService.getConnection();
             final Statement st = con.createStatement()) {
            st.setQueryTimeout(queryTimeout); // timeout in seconds
            st.setFetchSize(fetchSize); // hint fetch size
            final AtomicLong nrOfRows = new AtomicLong(0L);
            if (fileToProcess == null) {
                fileToProcess = session.create();
            }
            fileToProcess = session.write(fileToProcess, out -> {
                try {
                    logger.debug("Executing query {}", new Object[]{selectQuery});
                    final ResultSet resultSet = st.executeQuery(selectQuery);
                    nrOfRows.set(JdbcCommon.convertToAvroStream(resultSet, out, convertNamesForAvro));
                } catch (final SQLException e) {
                    throw new ProcessException(e);
                }
            });

            // set attribute how many rows were selected
            fileToProcess = session.putAttribute(fileToProcess, RESULT_ROW_COUNT, String.valueOf(nrOfRows.get()));

            logger.info("{} contains {} Avro records; transferring to 'success'",
                        new Object[]{fileToProcess, nrOfRows.get()});
            session.getProvenanceReporter().modifyContent(fileToProcess, "Retrieved " + nrOfRows.get() + " rows",
                                                          stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(fileToProcess, SUCCESS);
        } catch (final ProcessException | SQLException e) {
            if (fileToProcess == null) {
                // This can happen if any exceptions occur while setting up the connection, statement, etc.
                logger.error("Unable to execute SQL select query {} due to {}. No FlowFile to route to failure",
                             new Object[]{selectQuery, e});
                context.yield();
            } else {
                if (context.hasIncomingConnection()) {
                    logger.error("Unable to execute SQL select query {} for {} due to {}; routing to failure",
                                 new Object[]{selectQuery, fileToProcess, e});
                    fileToProcess = session.penalize(fileToProcess);
                } else {
                    logger.error("Unable to execute SQL select query {} due to {}; routing to failure",
                                 new Object[]{selectQuery, e});
                    context.yield();
                }
                session.transfer(fileToProcess, FAILURE);
            }
        }
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Sets.newHashSet(SUCCESS, FAILURE);
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.of(DBCP_SERVICE, SQL_SELECT_QUERY, QUERY_TIMEOUT, FETCH_SIZE, NORMALIZE_NAMES_FOR_AVRO);
    }


    static {
        // Build relations
        SUCCESS = new Relationship.Builder()
                .name("success")
                .description("Successfully created FlowFile from SQL query result set.")
                .build();

        FAILURE = new Relationship.Builder()
                .name("failure")
                .description(
                        "SQL query execution failed. Incoming FlowFile will be penalized and routed to this relationship")
                .build();


        // Build properties
        DBCP_SERVICE = new PropertyDescriptor.Builder()
                .name("Database Connection Pooling Service")
                .description("The Controller Service that is used to obtain connection to database")
                .required(true)
                .identifiesControllerService(DBCPService.class)
                .build();

        SQL_SELECT_QUERY = new PropertyDescriptor.Builder()
                .name("SQL select query")
                .description(
                        "The SQL select query to execute. The query can be empty, a constant value, or built from attributes "
                                + "using Expression Language. If this property is specified, it will be used regardless of the content of "
                                + "incoming flowfiles. If this property is empty, the content of the incoming flow file is expected "
                                + "to contain a valid SQL select query, to be issued by the processor to the database. Note that Expression "
                                + "Language is not evaluated for flow file contents.")
                .required(false)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(true)
                .build();

        QUERY_TIMEOUT = new PropertyDescriptor.Builder()
                .name("Max Wait Time")
                .description("The maximum amount of time allowed for a running SQL select query "
                                     + " , zero means there is no limit. Max time less than 1 second will be equal to zero.")
                .defaultValue("0 seconds")
                .required(true)
                .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
                .sensitive(false)
                .build();

        NORMALIZE_NAMES_FOR_AVRO = new PropertyDescriptor.Builder()
                .name("dbf-normalize")
                .displayName("Normalize Table/Column Names")
                .description(
                        "Whether to change non-Avro-compatible characters in column names to Avro-compatible characters. For example, colons and periods "
                                + "will be changed to underscores in order to build a valid Avro record.")
                .allowableValues("true", "false")
                .defaultValue("false")
                .required(true)
                .build();

        FETCH_SIZE = new PropertyDescriptor.Builder()
                .name("fetch-size-hint")
                .displayName("Fetch Size")
                .description(
                        "The number of result rows to be fetched from the result set at a time. This is a hint to the driver and may not be "
                                + "honored and/or exact. If the value specified is zero, then the hint is ignored.")
                .defaultValue("1000")
                .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
                .required(false)
                .build();
    }
}