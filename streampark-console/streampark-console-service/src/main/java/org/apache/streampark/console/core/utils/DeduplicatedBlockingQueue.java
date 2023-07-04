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

package org.apache.streampark.console.core.utils;

import org.jetbrains.annotations.NotNull;

import java.util.HashSet;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;

public class DeduplicatedBlockingQueue extends LinkedBlockingQueue<Runnable> {

  private final HashSet<String> appIds;

  public DeduplicatedBlockingQueue(int capacity) {
    super(capacity);
    appIds = new HashSet<>();
  }

  @Override
  public AbstractMonitorTask take() throws InterruptedException {
    AbstractMonitorTask task = (AbstractMonitorTask) super.take();
    appIds.remove(task.getAppId());
    return task;
  }

  @Override
  public AbstractMonitorTask poll() {
    AbstractMonitorTask task = (AbstractMonitorTask) super.poll();
    if (!Objects.isNull(task)) {
      appIds.remove(task.getAppId());
    }
    return task;
  }

  @Override
  public boolean offer(@NotNull Runnable task) {

    if (appIds.contains(((AbstractMonitorTask) task).getAppId())) {
      return false;
    }
    return super.offer(task);
  }

  @Override
  public AbstractMonitorTask remove() {
    AbstractMonitorTask task = (AbstractMonitorTask) super.remove();
    appIds.remove(task.getAppId());
    return task;
  }
}
