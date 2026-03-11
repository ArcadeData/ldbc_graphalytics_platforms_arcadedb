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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;

/**
 * Wrapper class for the initialization and safe shutdown of an embedded ArcadeDB database.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ArcadeDBDatabase implements AutoCloseable {

    private final Database database;

    /**
     * Opens an existing embedded ArcadeDB database from the specified path.
     *
     * @param databasePath the path of the pre-loaded graph database
     */
    public ArcadeDBDatabase(String databasePath) {
        this.database = new DatabaseFactory(databasePath).open();
    }

    /**
     * @return a handle to the ArcadeDB database
     */
    public Database get() {
        return database;
    }

    @Override
    public void close() {
        database.close();
    }
}
