#!/bin/bash
#
# Copyright 2015 - 2026 Delft University of Technology / Arcade Data Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Ensure the configuration file exists
if [ ! -f "$config/platform.properties" ]; then
	echo "Missing mandatory configuration file: $config/platform.properties" >&2
	exit 1
fi

# Set JVM memory options
export java_opts="-Xms16g -Xmx16g --add-modules jdk.incubator.vector"

# Ensure runner subprocesses also get the incubator module
export JDK_JAVA_OPTIONS="--add-modules jdk.incubator.vector"

# Set library jar
export LIBRARY_JAR=`ls lib/graphalytics-*default*.jar`
