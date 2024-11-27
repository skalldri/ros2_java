// Copyright 2020 Open Source Robotics Foundation, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.ros2.rcljava.publisher.statuses;

import org.ros2.rcljava.utils.Supplier;

import org.ros2.rcljava.common.JNIUtils;
import org.ros2.rcljava.events.PublisherEventStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class serves as a bridge between a rmw_matched_status_t and RCLJava.
 */
public class Matched implements PublisherEventStatus {
  /**
   * The total cumulative count of subscribers matched to the concerned publisher.
   */
  public long totalCount;

  /**
   * totalCount change since last time the status was read.
   */
  public long totalCountChange;

  /**
   * The number of subscribers currently matched to the concerned publisher.
   */
  public long currentCount;

  /**
   * The current_count change since last time the status was read.
   */
  public long currentCountChange;

  public final long allocateRCLStatusEvent() {
    return nativeAllocateRCLStatusEvent();
  }

  public final void deallocateRCLStatusEvent(long handle) {
    nativeDeallocateRCLStatusEvent(handle);
  }

  public final void fromRCLEvent(long handle) {
    nativeFromRCLEvent(handle);
  }

  public final int getPublisherEventType() {
    return nativeGetPublisherEventType();
  }

  // TODO(ivanpauno): Remove this when -source 8 can be used (method references
  // for the win)
  public static final Supplier<Matched> factory = new Supplier<Matched>() {
    public Matched get() {
      return new Matched();
    }
  };

  private static final Logger logger = LoggerFactory.getLogger(Matched.class);
  static {
    try {
      JNIUtils.loadImplementation(Matched.class);
    } catch (UnsatisfiedLinkError ule) {
      logger.error("Native code library for 'Matched' failed to load.\n" + ule);
      System.exit(1);
    }
  }

  private static native long nativeAllocateRCLStatusEvent();

  private static native void nativeDeallocateRCLStatusEvent(long handle);

  private native void nativeFromRCLEvent(long handle);

  private static native int nativeGetPublisherEventType();
}