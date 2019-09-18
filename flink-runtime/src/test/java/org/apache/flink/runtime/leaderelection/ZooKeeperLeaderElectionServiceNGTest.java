/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.leaderelection;

import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.ZooKeeperLeaderRetrievalService;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.zookeeper.ZooKeeperTestEnvironment;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Unit tests for {@link ZooKeeperLeaderElectionServiceNG}.
 */
public class ZooKeeperLeaderElectionServiceNGTest extends TestLogger {
	private static final ZooKeeperTestEnvironment ZOOKEEPER = new ZooKeeperTestEnvironment(1);

	@Rule
	public TestName testName = new TestName();

	@After
	public void after() throws Exception {
		ZOOKEEPER.deleteAll();
	}

	@AfterClass
	public static void tearDown() throws Exception {
		ZOOKEEPER.shutdown();
	}

	@Test
	public void testPublishLeaderInfo() throws Exception {
		LeaderElectionService leaderElectionService = new ZooKeeperLeaderElectionServiceNG(
			ZOOKEEPER.getClient(),
			testName.getMethodName());
		LeaderRetrievalService leaderRetrievalService = new ZooKeeperLeaderRetrievalService(
			ZOOKEEPER.getClient(),
			ZooKeeperUtils.getLeaderInfoPath(testName.getMethodName()));

		try {
			String leaderAddress = "foo";
			TestingListener listener = new TestingListener();

			leaderRetrievalService.start(listener);
			leaderElectionService.start(new TestingContender(leaderAddress, leaderElectionService));

			listener.waitForNewLeader(2000L);
			assertThat(listener.getAddress(), is(leaderAddress));
		} finally {
			leaderRetrievalService.stop();
			leaderElectionService.stop();
		}
	}

	@Test
	public void testMultipleLeaders() throws Exception {
		LeaderElectionService leaderElectionService1 = new ZooKeeperLeaderElectionServiceNG(
			ZOOKEEPER.getClient(),
			testName.getMethodName());
		LeaderElectionService leaderElectionService2 = new ZooKeeperLeaderElectionServiceNG(
			ZOOKEEPER.getClient(),
			testName.getMethodName());
		LeaderRetrievalService leaderRetrievalService = new ZooKeeperLeaderRetrievalService(
			ZOOKEEPER.getClient(),
			ZooKeeperUtils.getLeaderInfoPath(testName.getMethodName()));

		try {
			TestingContender contender1 = new TestingContender("foo", leaderElectionService1);
			TestingContender contender2 = new TestingContender("bar", leaderElectionService2);
			TestingListener listener = new TestingListener();

			leaderRetrievalService.start(listener);
			leaderElectionService1.start(contender1);

			listener.waitForNewLeader(2000L);
			assertThat(listener.getAddress(), is(contender1.getAddress()));

			leaderElectionService2.start(contender2);
			leaderElectionService1.stop();

			listener.waitForNewLeader(2000L);
			assertThat(listener.getAddress(), is(contender2.getAddress()));
		} finally {
			leaderRetrievalService.stop();
			leaderElectionService1.stop();
			leaderElectionService2.stop();
		}
	}
}
