/* Copyright 2017-2018 Esteve Fernandez <esteve@apache.org>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.ros2.rcljava.executors;

import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import org.ros2.rcljava.RCLJava;
import org.ros2.rcljava.node.ComposableNode;
import org.ros2.rcljava.executors.BaseExecutor;

public class MultiThreadedExecutor implements org.ros2.rcljava.executors.Executor {
  private BaseExecutor baseExecutor;
  private ExecutorService threadpool;
  private Object mutex;
  private int numberOfThreads;
  private ReentrantReadWriteLock execLock = new ReentrantReadWriteLock(true /* fair */);
  private Lock r = execLock.readLock();
  private Lock w = execLock.writeLock();

  public MultiThreadedExecutor(int numberOfThreads) {
    this.baseExecutor = new BaseExecutor();
    this.threadpool = Executors.newFixedThreadPool(numberOfThreads);
    this.mutex = new Object();
    this.numberOfThreads = numberOfThreads;
  }

  public MultiThreadedExecutor() {
    this(Runtime.getRuntime().availableProcessors());
  }

  public void addNode(ComposableNode node) {
    this.baseExecutor.addNode(node);
  }

  public void removeNode(ComposableNode node) {
    this.baseExecutor.removeNode(node);
  }

  public void spinOnce() {
    spinOnce(-1);
  }

  public void spinOnce(long timeout) {
    this.baseExecutor.spinOnce(timeout);
  }

  public void spinSome() {
    this.spinSome(0);
  }

  public void spinSome(long maxDurationNs) {
    this.baseExecutor.spinSome(maxDurationNs);
  }

  public void spinAll(long maxDurationNs) {
    this.baseExecutor.spinAll(maxDurationNs);
  }

  public void spin() {
    synchronized (mutex) {
      for (int i = 0; i < this.numberOfThreads; i++) {
        this.threadpool.execute(new Runnable() {
          public void run() {
            MultiThreadedExecutor.this.run();
          }
        });
      }
    }
    this.threadpool.shutdown();
  }

  private void run() {
    // Original implementation
    /*
    while (RCLJava.ok()) {
      synchronized (mutex) {
        this.spinOnce();
      }
    }
    */

    // New, actual multi-threading implementation
    // Lets keep an eye out for concurrency issues

    // There are three main functions from BaseExecutor that we need to ensure are thread-safe here:
    // - BaseExecutor::getNextExecutable()
    // - BaseExecutor::waitForWork() 
    // - BaseExecutor::executeAnyExecutable() 
    //
    // waitForWork is probably the hardest. Lets check that first

    while (RCLJava.ok()) {
        // This appears to be thread-safe and performant, on closer inspection.
        //
        // TL;DR: 
        // Only a single thread will ever be waiting on the waitForWork() call. Other threads will 
        // pile up behind that single thread on the synchronization barrier. As the threads acquire work, they will
        // one-at-a-time enter the sync block and either pickup work to do, or block all other threads until new work
        // becomes available.
        //
        // When new work becomes available, it gets "discovered" in waitForWork(). This populates internal
        // variables inside BaseExecutor that contains a list of all discovered work.
        // While there is still non-executed "discovered" work, getNextExecutable() will return a non-NULL value.
        //
        // When work is returned from getNextExecutable(), it gets marked as "invalid" so it doesn't get double-returned.
        // When the work has been completed, it gets deleted from the list of "discovered" work.
        //
        // This looks generally thread-safe, but there is one edge-case that needs to be accounted for.
        // Say one thread is in the middle of executing the last work discovered during the previous call to waitForWork().
        // The work-in-progress is currently marked "invalid", but is still being tracked in the list of "discovered" work.
        // Now, another thread finishes its work, finds that getNextExecutable() returns NULL, and then calls waitForWork() to
        // try and discover more work to do. This causes the list of "discovered" work to be destroyed, which means the work-in-progress
        // could get double-executed, or cause a crash when we complete the work-in-progress and try to delete the entry from the list of
        // discovered work.
        //
        // To fix this, we need to prevent waitForWork() from recreating the list of discovered work until all outstanding 
        // work has been completed. We implement this with a reader-writer lock: we consider executeAnyExecutable() to be a
        // "read" operation, and an unlimited number are allowed to operate in parallel. The waitForWork() operation is a 
        // "write" operation, which must operate in a single-threaded manner with no outstanding reads.

        //
        // TL:
        // - Only one thread can call getNextExecutable() (more importantly) waitForWork() at a time.
        // - There are two options:
        //   - We have work to do (maybe lots of work?) and getNextExecutable() returns a valid work item
        //     - In this case, we quickly exit the mutex block and start executing the work. Other threads
        //       are allowed to call getNextExecutable() while we're busy executing the work. Other threads
        //       may find that there is no more work to do, which will cause them to block in waitForWork().
        //       After we complete execution, we will get blocked by the mutex and the thread will quiesce
        //   - We have no work to do, and we block forever inside waitForWork()
        //     - In this case, all other threads will get blocked at the mutex sync barrier. But this is OK, 
        //       since we're determined there's no actual work to do, so the threads wouldn't be doing anything anyway.
        // 
        AnyExecutable anyExecutable = null;
        synchronized (mutex) {
          // Check to see if there's any available work to do.
          // This modifies the underlying list of work. We need to execute it under lock, 
          // so that other threads don't try to run the same work twice or end up in wonky states
          anyExecutable = this.baseExecutor.getNextExecutable();
          
          // We didn't get an executable. Wait for work instead.
          if (anyExecutable == null) {
            // Take the writer lock. This will block until all instances of executeAnyExecutable() have completed,
            // which guarantees that it's safe to destroy the internal bookkeeping list of "discovered" work.
            this.w.lock();

            this.baseExecutor.waitForWork(-1);

            this.w.unlock();

            anyExecutable = this.baseExecutor.getNextExecutable();
          }
          // We got an executable that should be executed. Take the reader lock to ensure that
          // no one calls waitForWork() before we get a chance to execute it
          this.r.lock();
        }

        // We must have work to do at this point
        // TODO: if we hit his in reality, there's jank somewhere in waitForWork().
        // Consider switching above if(anyExecutable == null) to a while(anyExecutable == null)
        assert(anyExecutable != null);

        this.baseExecutor.executeAnyExecutable(anyExecutable);

        // Unlock the reader lock now that we're done executing the work. This will unblock
        // any threads waiting to call waitForWork() to acqurire more work to do.
        this.r.unlock();
    }
  }
}
