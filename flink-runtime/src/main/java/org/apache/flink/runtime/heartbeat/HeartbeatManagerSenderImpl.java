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

package org.apache.flink.runtime.heartbeat;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * {@link HeartbeatManager} implementation which regularly requests a heartbeat response from its
 * monitored {@link HeartbeatTarget}. The heartbeat period is configurable.
 *
 * @param <I> Type of the incoming heartbeat payload
 * @param <O> Type of the outgoing heartbeat payload
 */
public class HeartbeatManagerSenderImpl<I, O> extends HeartbeatManagerImpl<I, O>
        implements Runnable {

    private final long heartbeatPeriod;

    HeartbeatManagerSenderImpl(
            long heartbeatPeriod,
            long heartbeatTimeout,
            int failedRpcRequestsUntilUnreachable,
            ResourceID ownResourceID,
            HeartbeatListener<I, O> heartbeatListener,
            ScheduledExecutor mainThreadExecutor,
            Logger log) {
        this(
                heartbeatPeriod,
                heartbeatTimeout,
                failedRpcRequestsUntilUnreachable,
                ownResourceID,
                heartbeatListener,
                mainThreadExecutor,
                log,
                new HeartbeatMonitorImpl.Factory<>());
    }

    HeartbeatManagerSenderImpl(
            long heartbeatPeriod,
            long heartbeatTimeout,
            int failedRpcRequestsUntilUnreachable,
            ResourceID ownResourceID,
            HeartbeatListener<I, O> heartbeatListener,
            ScheduledExecutor mainThreadExecutor,
            Logger log,
            HeartbeatMonitor.Factory<O> heartbeatMonitorFactory) {
        super(
                heartbeatTimeout,
                failedRpcRequestsUntilUnreachable,
                ownResourceID,
                heartbeatListener,
                mainThreadExecutor,
                log,
                heartbeatMonitorFactory);

        this.heartbeatPeriod = heartbeatPeriod;
        // TODO_LL :启动一个定时任务，立马执行this 这个任务
        mainThreadExecutor.schedule(this, 0L, TimeUnit.MILLISECONDS);
    }

    /**
     * 每隔10s 执行一次循环
     */
    @Override
    public void run() {
        if (!stopped) {
            log.debug("Trigger heartbeat request.");
            /**
             *  TODO_LL :1:给所有的心跳组件发送rpc 请求
             *            ResourceManager 给所有注册成功的从节点（taskExecutor）发送 requestHeartbeat 这个rpc请求
             *            ResourceManager 给所有注册成功的主节点（jobMaster）发送 requestHeartbeat 这个rpc请求
             *            jobMaster 给所有注册成功的从节点（taskExecutor）发送 requestHeartbeat 这个rpc请求
             */
            for (HeartbeatMonitor<O> heartbeatMonitor : getHeartbeatTargets().values()) {
                requestHeartbeat(heartbeatMonitor);
            }

            getMainThreadExecutor().schedule(this, heartbeatPeriod, TimeUnit.MILLISECONDS);
        }
    }

    private void requestHeartbeat(HeartbeatMonitor<O> heartbeatMonitor) {
        /**
         *  TODO_LL :每个角色注册成功的时候，会在对应的管理组件中生成一个HeartbeatTarget

         */
        O payload = getHeartbeatListener().retrievePayload(heartbeatMonitor.getHeartbeatTargetId());
        final HeartbeatTarget<O> heartbeatTarget = heartbeatMonitor.getHeartbeatTarget();

        heartbeatTarget
                .requestHeartbeat(getOwnResourceID(), payload)
                .whenCompleteAsync(
                        handleHeartbeatRpc(heartbeatMonitor.getHeartbeatTargetId()),
                        getMainThreadExecutor());
    }
}
