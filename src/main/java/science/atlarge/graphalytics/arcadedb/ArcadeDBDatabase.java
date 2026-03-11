/*
 * Copyright 2015 - 2026 Delft University of Technology / Arcade Data Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package science.atlarge.graphalytics.arcadedb;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

/**
 * Wrapper class for managing a Bolt connection to an ArcadeDB server.
 * Uses the Neo4j Java driver to connect via the Bolt protocol.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ArcadeDBDatabase implements AutoCloseable {

    private final Driver driver;
    private final String databaseName;

    /**
     * Initializes a Bolt connection to the ArcadeDB server.
     *
     * @param boltUri      the Bolt URI (e.g., bolt://localhost:7687)
     * @param databaseName the name of the ArcadeDB database
     * @param username     the username for authentication
     * @param password     the password for authentication
     */
    public ArcadeDBDatabase(String boltUri, String databaseName, String username, String password) {
        this.driver = GraphDatabase.driver(boltUri, AuthTokens.basic(username, password));
        this.databaseName = databaseName;
    }

    /**
     * @return a new Bolt session connected to the database
     */
    public Session getSession() {
        return driver.session(SessionConfig.forDatabase(databaseName));
    }

    /**
     * @return the database name
     */
    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public void close() {
        driver.close();
    }
}
