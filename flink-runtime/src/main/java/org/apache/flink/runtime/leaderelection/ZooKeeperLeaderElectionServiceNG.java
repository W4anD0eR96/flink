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

import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.util.FlinkException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.recipes.locks.LockInternals;
import org.apache.curator.framework.recipes.locks.LockInternalsSorter;
import org.apache.curator.framework.recipes.locks.StandardLockInternalsDriver;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Implementation of {@link LeaderElectionService} which deploys ZooKeeper for
 * leader election and data storage.
 *
 * <p>
 * The services store data in znodes as illustrated by the following tree structure:
 * </p>
 *
 * <pre>
 * /flink/cluster-id-1/dispatcher/registry/latch-1
 * 	    |            |          |         /latch-2
 * 	    |            |          |         /latch-3
 * 	    |            |          +/info
 * 	    |            |          +/store/
 * 	    |            +/resource-manager/registry/latch-1
 * 	    |            |                          /latch-2
 * 	    |            +/job-id-1/job-manager/registry/latch-1
 * 	    |                                  /info
 * 	    |                                  /store/
 * 	    +/cluster-id-2/
 *  * </pre>
 */
public class ZooKeeperLeaderElectionServiceNG implements LeaderElectionService, UnhandledErrorListener, ConnectionStateListener {

	private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperLeaderElectionServiceNG.class);
	private static final LockInternalsSorter SORTER = StandardLockInternalsDriver::standardFixForSorting;
	private static final String LOCK_NAME = "latch-";

	private final Object lock = new Object();

	private final CuratorFramework client;

	private final String leaderRegistryPath;

	private final String leaderInfoPath;

	private final String leaderStorePath;

	private State state;

	private LeaderContender listener;

	@GuardedBy("lock")
	private String leaderLatchPath;

	private volatile UUID leaderSessionID;

	public ZooKeeperLeaderElectionServiceNG(CuratorFramework client, String basePath) {
		this.client = client;
		this.state = State.CREATED;
		this.leaderLatchPath = null;
		this.leaderInfoPath = ZooKeeperUtils.getLeaderInfoPath(basePath);
		this.leaderStorePath = ZooKeeperUtils.getLeaderStorePath(basePath);
		this.leaderRegistryPath = ZooKeeperUtils.getLeaderRegistryPath(basePath);
	}

	@Override
	public void start(@Nonnull LeaderContender listener) throws Exception {
		synchronized (lock) {
			checkState(state == State.CREATED, "The leader election service is already started.");

			this.listener = checkNotNull(listener);
			this.client.getConnectionStateListenable().addListener(this);
			this.client.getUnhandledErrorListenable().addListener(this);
			this.client.createContainers(leaderStorePath);

			resetLeaderLatch();
		}
	}


	@Override
	public void stop() {
		synchronized (lock) {
			if (state == State.STOPPED) {
				return;
			}

			changeState(State.STOPPED);

			try {
				setLeaderLatch(null);
			} catch (Throwable t) {
				LOG.warn("Could not properly remove the leader latch.", t);
			}

			try {
				client.getConnectionStateListenable().removeListener(this);
			} catch (Throwable t) {
				LOG.warn("Could not properly remove the connection state listener.", t);
			}

			try {
				client.getUnhandledErrorListenable().removeListener(this);
			} catch (Throwable t) {
				LOG.warn("Could not properly remove the listener on unhandled errors.", t);
			}

			listener = null;
		}
	}

	@Override
	public boolean hasLeadership(@Nonnull UUID leaderSessionId) {
		return leaderSessionId.equals(this.leaderSessionID);
	}

	@Override
	public void confirmLeaderSessionID(@Nonnull UUID leaderSessionID) {
		String localLeaderLatchPath = getLeaderLatchPathForModification();

		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);

			oos.writeUTF(listener.getAddress());
			oos.writeObject(leaderSessionID);

			oos.close();

			Stat stat = client.checkExists().forPath(leaderInfoPath);

			if (stat != null) {
				client.inTransaction()
					.check().forPath(localLeaderLatchPath).and()
					.setData().forPath(leaderInfoPath, baos.toByteArray()).and()
					.commit();
			} else {
				client.inTransaction()
					.check().forPath(localLeaderLatchPath).and()
					.create().withMode(CreateMode.PERSISTENT).forPath(leaderInfoPath, baos.toByteArray()).and()
					.commit();
			}
		} catch (Exception e) {
			listener.handleError(e);
		}
	}

	public void concealLeaderInfo() throws Exception {
		String localLeaderLatchPath = getLeaderLatchPathForModification();

		client.inTransaction()
			.check().forPath(localLeaderLatchPath).and()
			.delete().forPath(leaderInfoPath).and()
			.commit();
	}

	@Override
	public void unhandledError(String s, Throwable throwable) {
		handleException(
			new FlinkException("Caught unhandled exception in curator: " + s, throwable));
	}

	private void handleException(Exception exception) {
		synchronized (lock) {
			if (listener != null) {
				listener.handleError(exception);
			}
		}
	}

	private String getLeaderLatchPathForModification() {
		synchronized (lock) {
			checkState(state == State.LEADING, "Cannot modify state without granted leadership.");
			return leaderLatchPath;
		}
	}

	@Override
	public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
		switch (connectionState) {
			case RECONNECTED:
				LOG.warn("Connection to ZooKeeper was reconnected.");
				break;
			case SUSPENDED:
				LOG.warn("Connection to ZooKeeper was suspended.");
				break;
			case LOST:
				LOG.error("Connection to ZooKeeper was lost.");

				// session expired, we don't want to recover from this exception
				stop();

				break;
			default:
				break;
		}
	}

	private void setLeaderLatch(String newValue) throws Exception {
		String oldValue = leaderLatchPath;
		leaderLatchPath = newValue;

		if (oldValue != null) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Delete leader latch {}. Reported by listener {}.", oldValue, listener);
			}

			client.delete().inBackground((client, event) -> {
				if (event.getResultCode() != KeeperException.Code.NONODE.intValue()
					&& event.getResultCode() != KeeperException.Code.OK.intValue()) {
					handleException(new FlinkException("Cannot properly delete leader latch " + event.getPath()));
				}
			}).forPath(oldValue);
		}
	}

	private void changeState(State newState) {
		State oldState = state;

		if (oldState != State.LEADING && newState == State.LEADING) {
			state = newState;
			if (listener != null) {
				leaderSessionID = UUID.randomUUID();
				listener.grantLeadership(leaderSessionID);
				LOG.info("{} has been granted leadership.", listener);
			}
		} else if (oldState == State.LEADING && newState != State.LEADING) {
			if (listener != null) {
				listener.revokeLeadership();
				LOG.info("{} has been revoked leadership.", listener);
			}
			state = newState;
		} else {
			state = newState;
		}
	}

	private void resetLeaderLatch() {
		try {
			setLeaderLatch(null);

			client.create()
				.creatingParentContainersIfNeeded()
				.withProtection()
				.withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
				.inBackground((client, event) -> {
					if (event.getResultCode() == KeeperException.Code.OK.intValue()) {
						onLeaderLatchCreated(event.getName());
					} else {
						handleException(
							new FlinkException("Cannot properly create the " +
								"leader latch (rc: " + event.getResultCode() +
								")."));
					}
				}).forPath(ZKPaths.makePath(leaderRegistryPath, LOCK_NAME));

			changeState(State.REGISTERING);
		} catch (Throwable t) {
			handleException(
				new FlinkException("Could not properly create leader latch.", t));
		}
	}

	private void onLeaderLatchCreated(String leaderLatchPath) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Successfully create the leader latch {}.", leaderLatchPath);
		}

		synchronized (lock) {
			try {
				setLeaderLatch(leaderLatchPath);
			} catch (Throwable t) {
				handleException(new FlinkException("Could not properly set the leader latch.", t));
			}

			if (state == State.REGISTERING) {
				getAllLeaderLatches();
			} else {
				LOG.warn(
					"Unexpected state ({}) when the leader latch is created. Delete leader latch {}.",
					state,
					leaderLatchPath);
				try {
					setLeaderLatch(null);
				} catch (Throwable t) {
					handleException(new FlinkException("Could not properly unset the leader latch.", t));
				}
			}
		}
	}


	private void getAllLeaderLatches() {
		try {
			client.getChildren().inBackground((client, event) -> {
				if (event.getResultCode() == KeeperException.Code.OK.intValue()) {
					onAllLeaderLatchesGotten(event.getChildren());
				} else {
					handleException(
						new FlinkException("Cannot properly get the leader " +
							"latches (rc: " + event.getResultCode() + ")."));
				}
			}).forPath(leaderRegistryPath);

			changeState(State.ELECTING);
		} catch (Throwable t) {
			handleException(new FlinkException("Could not properly get all leader latches.", t));
		}
	}

	private void onAllLeaderLatchesGotten(List<String> leaderLatchPaths) {
		synchronized (lock) {
			if (state == State.STOPPED) {
				return;
			}

			if (state == State.ELECTING) {
				List<String> sortedChildren = LockInternals.getSortedChildren(LOCK_NAME, SORTER, leaderLatchPaths);
				int index = leaderLatchPath != null ? sortedChildren.indexOf(ZKPaths.getNodeFromPath(leaderLatchPath)) : -1;

				if (LOG.isDebugEnabled()) {
					LOG.debug(
						"On all leader latches gotten {}. Our leader latch is {}({}). Reported by listener {}.",
						sortedChildren,
						leaderLatchPath,
						index,
						listener);
				}

				if (index < 0) {
					handleException(new FlinkException("Leader latch has gone unexpectedly."));
				} else if (index == 0) {
					changeState(State.LEADING);
				} else {
					watchPrecedingLeaderLatch(sortedChildren.get(index - 1));
				}
			} else {
				LOG.warn("Unexpected state when gotten all latches: {}.", state);
			}
		}
	}

	private void watchPrecedingLeaderLatch(String precedingLeaderLatchPath) {
		try {
			client.getData()
				.usingWatcher((Watcher) event -> {
					if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
						if (LOG.isDebugEnabled()) {
							LOG.debug(
								"On proceeding leader latch missing {}. Reported by listener {}.",
								event.getPath(),
								listener);
						}

						try {
							onPrecedingLeaderLatchMissing();
						} catch (Throwable t) {
							handleException(new FlinkException("Could not properly handle watch events.", t));
						}
					}
				})
				.inBackground((client, event) -> {
					if (event.getResultCode() == KeeperException.Code.NONODE.intValue()) {
						onPrecedingLeaderLatchMissing();
					}
				})
				.forPath(ZKPaths.makePath(leaderRegistryPath, precedingLeaderLatchPath));

			changeState(State.WAITING);
		} catch (Throwable t) {
			handleException(new FlinkException("Could not properly watch the preceding leader latch.", t));
		}
	}

	private void onPrecedingLeaderLatchMissing() {
		synchronized (lock) {
			if (state == State.WAITING) {
				getAllLeaderLatches();
			} else {
				LOG.warn("Unexpected state ({}) when the preceding leader latch is deleted.", state);
			}
		}
	}

	/**
	 * States that {@link ZooKeeperLeaderElectionService} switch among.
	 */
	private enum State {
		/**
		 * The initial state.
		 *
		 * <ul>
		 * 		<li>Transit to REGISTERING on {@link #start(LeaderContender)}} called.</li>
		 * 		<li>Transit to STOPPED on {@link #stop()} called.</li>
		 * </ul>
		 */
		CREATED,

		/**
		 * Transited to this state, {@link #resetLeaderLatch()} would be called to
		 * create the leader latch for contending leadership.
		 *
		 * <ul>
		 * 		<li>Transit to ELECTING {@link #onLeaderLatchCreated(String)}.</li>
		 * 		<li>Transit to STOPPED on {@link ConnectionState#LOST}.</li>
		 * 		<li>Transit to STOPPED on {@link #stop()} called.</li>
		 * </ul>
		 */
		REGISTERING,

		/**
		 * Transited to this state, {@link #getAllLeaderLatches()} would be called to
		 * check if the contender became the leader.
		 *
		 * <ul>
		 * 		<li>Transit to LEADING {@link #onAllLeaderLatchesGotten(List)} and the contender
		 * 			became the leader, i.e., the latch path has the least sequential number.</li>
		 * 		<li>Transit to WAITING {@link #onAllLeaderLatchesGotten(List)} and the contender
		 * 		    was not the leader.</li>
		 * 		<li>Transit to REGISTERING {@link #onAllLeaderLatchesGotten(List)} and the latch
		 * 	 	    got lost. This is rare but possible.</li>
		 * 		<li>Transit to STOPPED on {@link ConnectionState#LOST}.</li>
		 * 		<li>Transit to STOPPED on {@link #stop()} called.</li>
		 * </ul>
		 */
		ELECTING,

		/**
		 * Transited to this state, {@link LeaderContender#grantLeadership)} would be
		 * called to grant leadership to the contender.
		 *
		 * <ul>
		 *     <li>Transit to STOPPED on {@link ConnectionState#LOST}.</li>
		 *     <li>Transit to STOPPED on {@link #stop()} called.</li>
		 * </ul>
		 */
		LEADING,

		/**
		 * Transited to this state, {@link #watchPrecedingLeaderLatch(String)} would be
		 * called to watch preceding leader latch and wait for leadership.
		 *
		 * <ul>
		 *     <li>Transit to ELECTING {@link #onPrecedingLeaderLatchMissing()}.</li>
		 *     <li>Transit to STOPPED on {@link ConnectionState#LOST}.</li>
		 *     <li>Transit to STOPPED on {@link #stop()} called.</li>
		 * </ul>
		 */
		WAITING,

		/**
		 * The final state which has no transition.
		 */
		STOPPED
	}
}
