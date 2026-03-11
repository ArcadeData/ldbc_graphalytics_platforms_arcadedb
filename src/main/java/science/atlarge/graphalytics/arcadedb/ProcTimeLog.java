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

import java.time.Instant;

/**
 * Responsible for logging the processing start and end time.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class ProcTimeLog {
    static final String START_PROC_TIME = "Processing starts at";
    static final String END_PROC_TIME = "Processing ends at";

    public static void start() {
        System.out.println(START_PROC_TIME + " " + Instant.now().toEpochMilli());
    }

    public static void end() {
        System.out.println(END_PROC_TIME + " " + Instant.now().toEpochMilli());
    }
}
