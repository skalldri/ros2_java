// Copyright 2016-2018 Esteve Fernandez <esteve@apache.org>
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

#include <jni.h>

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <chrono>
#include <iostream>

#include "rcl/error_handling.h"
#include "rcl/event.h"
#include "rcl/node.h"
#include "rcl/rcl.h"
#include "rmw/rmw.h"

#include "rcljava_common/exceptions.hpp"
#include "rcljava_common/signatures.hpp"

#ifdef __ANDROID__

#include <android/log.h>

#define TAG "PublisherImpl"

#define LOGE(...) __android_log_print(ANDROID_LOG_ERROR, TAG, __VA_ARGS__)
#define LOGW(...) __android_log_print(ANDROID_LOG_WARN, TAG, __VA_ARGS__)
#define LOGI(...) __android_log_print(ANDROID_LOG_INFO, TAG, __VA_ARGS__)
#define LOGD(...) __android_log_print(ANDROID_LOG_DEBUG, TAG, __VA_ARGS__)

#else

#define LOGE(...)
#define LOGW(...)
#define LOGI(...)
#define LOGD(...)

#endif

#include "org_ros2_rcljava_publisher_PublisherImpl.h"

using rcljava_common::exceptions::rcljava_throw_exception;
using rcljava_common::exceptions::rcljava_throw_rclexception;
using rcljava_common::signatures::convert_from_java_signature;
using rcljava_common::signatures::destroy_ros_message_signature;

JNIEXPORT void JNICALL
Java_org_ros2_rcljava_publisher_PublisherImpl_nativePublish(
  JNIEnv * env, jclass, jlong publisher_handle, jlong jmsg_destructor_handle, jobject jmsg)
{
  rcl_publisher_t * publisher = reinterpret_cast<rcl_publisher_t *>(publisher_handle);

  jclass jmessage_class = env->GetObjectClass(jmsg);

  jmethodID mid = env->GetStaticMethodID(jmessage_class, "getFromJavaConverter", "()J");
  jlong jfrom_java_converter = env->CallStaticLongMethod(jmessage_class, mid);

  convert_from_java_signature convert_from_java =
    reinterpret_cast<convert_from_java_signature>(jfrom_java_converter);

  auto start = std::chrono::high_resolution_clock::now();
  void * raw_ros_message = convert_from_java(jmsg, nullptr);
  auto end = std::chrono::high_resolution_clock::now();

  std::chrono::duration<double> diff = end - start;
  // Warn on long (>10ms) convert_from_java time
  if (diff.count() * 1000.0 > 10.0) {
    LOGW("convert_from_java() time = %f ms", diff.count() * 1000.0);
  }

  start = std::chrono::high_resolution_clock::now();
  rcl_ret_t ret = rcl_publish(publisher, raw_ros_message, nullptr);
  end = std::chrono::high_resolution_clock::now();
  diff = end - start;

  // Warn on long (>50ms) publish time
  if (diff.count() * 1000.0 > 50.0) {
    LOGW("Long rcl_publish() time = %f ms", diff.count() * 1000.0);
  }

  destroy_ros_message_signature destroy_ros_message =
    reinterpret_cast<destroy_ros_message_signature>(jmsg_destructor_handle);
  destroy_ros_message(raw_ros_message);

  if (ret != RCL_RET_OK) {
    std::string msg = "Failed to publish: " + std::string(rcl_get_error_string().str);
    rcl_reset_error();
    rcljava_throw_rclexception(env, ret, msg);
  }
}

JNIEXPORT void JNICALL
Java_org_ros2_rcljava_publisher_PublisherImpl_nativeDispose(
  JNIEnv * env, jclass, jlong node_handle, jlong publisher_handle)
{
  if (publisher_handle == 0) {
    // everything is ok, already destroyed
    return;
  }

  if (node_handle == 0) {
    // TODO(esteve): handle this, node is null, but publisher isn't
    return;
  }

  rcl_node_t * node = reinterpret_cast<rcl_node_t *>(node_handle);

  rcl_publisher_t * publisher = reinterpret_cast<rcl_publisher_t *>(publisher_handle);

  assert(publisher != NULL);

  rcl_ret_t ret = rcl_publisher_fini(publisher, node);

  if (ret != RCL_RET_OK) {
    std::string msg = "Failed to destroy publisher: " + std::string(rcl_get_error_string().str);
    rcl_reset_error();
    rcljava_throw_rclexception(env, ret, msg);
  }
}

JNIEXPORT jlong JNICALL
Java_org_ros2_rcljava_publisher_PublisherImpl_nativeCreateEvent(
  JNIEnv * env, jclass, jlong publisher_handle, jint event_type)
{
  auto * publisher = reinterpret_cast<rcl_publisher_t *>(publisher_handle);
  if (!publisher) {
    rcljava_throw_exception(
      env, "java/lang/IllegalArgumentException", "passed rcl_publisher_t handle is NULL");
    return 0;
  }
  auto * event = static_cast<rcl_event_t *>(malloc(sizeof(rcl_event_t)));
  if (!event) {
    rcljava_throw_exception(env, "java/lang/OutOfMemoryError", "failed to allocate rcl_event_t");
    return 0;
  }
  *event = rcl_get_zero_initialized_event();
  rcl_ret_t ret = rcl_publisher_event_init(
    event, publisher, static_cast<rcl_publisher_event_type_t>(event_type));
  if (RCL_RET_OK != ret) {
    std::string msg = "Failed to create event: " + std::string(rcl_get_error_string().str);
    rcl_reset_error();
    rcljava_throw_rclexception(env, ret, msg);
    free(event);
    return 0;
  }
  return reinterpret_cast<jlong>(event);
}
