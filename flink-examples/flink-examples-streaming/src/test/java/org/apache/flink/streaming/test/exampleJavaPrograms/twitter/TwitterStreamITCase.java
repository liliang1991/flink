/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.test.exampleJavaPrograms.twitter;

import org.apache.flink.streaming.examples.twitter.TwitterExample;
import org.apache.flink.streaming.examples.twitter.util.TwitterExampleData;
import org.apache.flink.streaming.util.StreamingProgramTestBase;

/**
 * Tests for {@link TwitterExample}.
 */
public class TwitterStreamITCase extends StreamingProgramTestBase {
	protected String resultPath;

	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(TwitterExampleData.STREAMING_COUNTS_AS_TUPLES, resultPath);
	}

	@Override
	protected void testProgram() throws Exception {
		TwitterExample.main(new String[]{"--output", resultPath});
	}

}
