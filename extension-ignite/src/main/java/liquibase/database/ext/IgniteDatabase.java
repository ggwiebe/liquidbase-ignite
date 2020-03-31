/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package liquibase.database.ext;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import liquibase.CatalogAndSchema;
import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.DatabaseConnection;
import liquibase.exception.DatabaseException;
import liquibase.logging.LogFactory;
import liquibase.logging.Logger;
import liquibase.statement.DatabaseFunction;
import liquibase.structure.core.Schema;

/**
 * @author Glenn Wiebe
 * @since 0.0.1
 */
public class IgniteDatabase extends AbstractJdbcDatabase {

    private static final String PRODUCT_NAME = "Ignite";
    private static final Logger log = LogFactory.getInstance().getLog();;
    public static final int MINIMUM_DBMS_MAJOR_VERSION = 2;
    public static final int MINIMUM_DBMS_MINOR_VERSION = 8;
    public static final int IGNITE_DEFAULT_PORT = 10800;


    private static String START_CONCAT = "CONCAT(";
    private static String END_CONCAT = ")";
    private static String SEP_CONCAT = ", ";
    private static List keywords = Arrays.asList(
            "CROSS",
            "CURRENT_DATE",
            "CURRENT_TIME",
            "CURRENT_TIMESTAMP",
            "DISTINCT",
            "EXCEPT",
            "EXISTS",
            "FALSE",
            "FETCH",
            "FOR",
            "FROM",
            "FULL",
            "GROUP",
            "HAVING",
            "INNER",
            "INTERSECT",
            "IS",
            "JOIN",
            "LIKE",
            "LIMIT",
            "MINUS",
            "NATURAL",
            "NOT",
            "NULL",
            "OFFSET",
            "ON",
            "ORDER",
            "PRIMARY",
            "ROWNUM",
            "SELECT",
            "SYSDATE",
            "SYSTIME",
            "SYSTIMESTAMP",
            "TODAY",
            "TRUE",
            "UNION",
            "UNIQUE",
            "WHERE");
    private String connectionSchemaName = "PUBLIC";

    public IgniteDatabase() {
        super.unquotedObjectsAreUppercased=true;
        super.setCurrentDateTimeFunction("NOW()");
        // for current date
        this.dateFunctions.add(new DatabaseFunction("CURRENT_DATE"));
        this.dateFunctions.add(new DatabaseFunction("CURDATE"));
        this.dateFunctions.add(new DatabaseFunction("SYSDATE"));
        this.dateFunctions.add(new DatabaseFunction("TODAY"));
        // for current time
        this.dateFunctions.add(new DatabaseFunction("CURRENT_TIME"));
        this.dateFunctions.add(new DatabaseFunction("CURTIME"));
        // for current timestamp
        this.dateFunctions.add(new DatabaseFunction("CURRENT_TIMESTAMP"));
        this.dateFunctions.add(new DatabaseFunction("NOW"));

        super.sequenceNextValueFunction = "NEXTVAL('%s')";
        super.sequenceCurrentValueFunction = "CURRVAL('%s')";
        // According to http://www.h2database.com/html/datatypes.html, retrieved on 2017-06-05
        super.unmodifiableDataTypes.addAll(Arrays.asList("int", "integer", "mediumint", "int4", "signed", "boolean",
                "bit", "bool", "tinyint", "smallint", "int2", "year", "bigint", "int8", "identity", "float", "float8",
                "real", "float4", "time", "date", "timestamp", "datetime", "smalldatetime", "timestamp with time zone",
                "other", "uuid", "array", "geometry"));
    }

    /* (non-Javadoc)
     * @see liquibase.database.Database#getDefaultDriver(java.lang.String)
     */
     public String getDefaultDriver(String url) {
         if (url.startsWith("jdbc:ignite:thin")) { //jdbc:ignite:thin://localhost:10836/HR
             return "org.apache.ignite.IgniteJdbcThinDriver";
         }
         return null;
     }


    /* (non-Javadoc)
     * @see liquibase.database.Database#getDefaultPort()
     */
    public Integer getDefaultPort() {
        return IGNITE_DEFAULT_PORT;
    }

    /* (non-Javadoc)
     * @see liquibase.database.Database#getShortName()
     */
    public String getShortName() {
        return PRODUCT_NAME.toLowerCase();
    }

    /* (non-Javadoc)
     * @see liquibase.database.Database#isCorrectDatabaseImplementation(liquibase.database.DatabaseConnection)
     */
    public boolean isCorrectDatabaseImplementation(DatabaseConnection connection) throws DatabaseException {
        return PRODUCT_NAME.equals(connection.getDatabaseProductName());
    }

    /* (non-Javadoc)
     * @see liquibase.database.Database#supportsInitiallyDeferrableColumns()
     */
    public boolean supportsInitiallyDeferrableColumns() {
        return false;
    }

    /* (non-Javadoc)
     * @see liquibase.database.Database#supportsTablespaces()
     */
    public boolean supportsTablespaces() {
        return false;
    }

    @Override
    public boolean supportsSequences() {
        return false;
    }

    /* (non-Javadoc)
     * @see liquibase.servicelocator.PrioritizedService#getPriority()
     */
    public int getPriority() {
        return PRIORITY_DEFAULT;
    }

    /* (non-Javadoc)
     * @see liquibase.database.AbstractJdbcDatabase#getDefaultDatabaseProductName()
     */
    @Override
    protected String getDefaultDatabaseProductName() {
        return PRODUCT_NAME;
    }

    @Override
    public void setConnection(DatabaseConnection conn) {
        //setReserverdWords();
        super.setConnection(conn);
    }


    @Override
    public boolean supportsSchemas() {
        return false;
    }

    @Override
    public boolean supportsAutoIncrement() {
        return false;
    }

    @Override
    public boolean supportsDDLInTransaction() {
        return false;
    }

    @Override
    public boolean supportsPrimaryKeyNames() {
        return true;
    }

    @Override
    public boolean jdbcCallsCatalogsSchemas() {
        return true;
    }

    @Override
    public String getJdbcCatalogName(CatalogAndSchema schema) {
        return null;
    }

    @Override
    public String getJdbcSchemaName(CatalogAndSchema schema) {
        return correctObjectName(schema.getCatalogName() == null ? schema.getSchemaName() : schema.getCatalogName(),
                Schema.class);
    }

    @Override
    public String getDefaultCatalogName() {// NOPMD
        return super.getDefaultCatalogName() == null ? null : super.getDefaultCatalogName().toUpperCase();
    }

//////////////////////////////////////////////////////////////////////////////////////////

    @Override
    public boolean isReservedWord(String objectName) {
        return keywords.contains(objectName.toUpperCase(Locale.US));
    }

}
