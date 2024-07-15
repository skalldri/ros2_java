@# Included from rosidl_generator_java/resource/idl.cpp.em
@{
from collections import defaultdict

from rosidl_generator_c import basetype_to_c
from rosidl_generator_c import idl_structure_type_to_c_include_prefix
from rosidl_generator_java import constructor_signatures
from rosidl_generator_java import get_java_type
from rosidl_generator_java import get_jni_signature
from rosidl_generator_java import get_jni_primitive_signature
from rosidl_generator_java import get_jni_type
from rosidl_generator_java import get_jni_array_cpp_type
from rosidl_generator_java import get_jni_array_func
from rosidl_generator_java import get_normalized_type
from rosidl_generator_java import value_methods
from rosidl_parser.definition import AbstractGenericString
from rosidl_parser.definition import AbstractString
from rosidl_parser.definition import AbstractWString
from rosidl_parser.definition import AbstractNestedType
from rosidl_parser.definition import AbstractSequence
from rosidl_parser.definition import Array
from rosidl_parser.definition import BasicType
from rosidl_parser.definition import NamespacedType

msg_normalized_type = '__'.join(message.structure.namespaced_type.namespaced_name())
msg_jni_type = '/'.join(message.structure.namespaced_type.namespaced_name())

list_java_type = "java.util.List"
array_list_java_type = "java.util.ArrayList"

list_normalized_type = "java__util__List"
list_jni_type = "java/util/List"

array_list_normalized_type = "java__util__ArrayList"
array_list_jni_type = "java/util/ArrayList"

# Collect JNI types and includes
cache = defaultdict(lambda: False)
cache[msg_normalized_type] = msg_jni_type
namespaced_types = set()
member_includes = set()
for member in message.structure.members:
    type_ = member.type
    if isinstance(type_, AbstractNestedType):
        cache[list_normalized_type] = list_jni_type
        cache[array_list_normalized_type] = array_list_jni_type
        type_ = type_.value_type
        if isinstance(type_, BasicType):
            member_includes.add('rosidl_runtime_c/primitives_sequence.h')
            member_includes.add('rosidl_runtime_c/primitives_sequence_functions.h')

    # We do not cache strings because java.lang.String behaves differently
    if not isinstance(type_, AbstractGenericString):
        cache[get_normalized_type(type_)] = get_jni_type(type_)

    if isinstance(type_, AbstractString):
        member_includes.add('rosidl_runtime_c/string.h')
        member_includes.add('rosidl_runtime_c/string_functions.h')

    if isinstance(type_, AbstractWString):
        member_includes.add('rosidl_runtime_c/u16string.h')
        member_includes.add('rosidl_runtime_c/u16string_functions.h')

    if isinstance(type_, NamespacedType):
        namespaced_types.add(get_jni_type(type_))
        include_prefix = idl_structure_type_to_c_include_prefix(type_)
        # TODO(jacobperron): Remove this logic after https://github.com/ros2/rosidl/pull/432 (Foxy)
        #                    and https://github.com/ros2/rosidl/pull/538
        # Strip off any service or action suffix
        # There are several types that actions and services are composed of, but they are included
        # a common header that is based on the action or service name
        # ie. there are not separate headers for each type
        if include_prefix.endswith('__request'):
            include_prefix = include_prefix[:-9]
        elif include_prefix.endswith('__response'):
            include_prefix = include_prefix[:-10]
        if include_prefix.endswith('__goal'):
            include_prefix = include_prefix[:-6]
        elif include_prefix.endswith('__result'):
            include_prefix = include_prefix[:-8]
        elif include_prefix.endswith('__feedback'):
            include_prefix = include_prefix[:-10]
        elif include_prefix.endswith('__send_goal'):
            include_prefix = include_prefix[:-11]
        elif include_prefix.endswith('__get_result'):
            include_prefix = include_prefix[:-12]
        member_includes.add(include_prefix + '.h')
}@
@{
# TODO(jacobperron): Remove this logic after https://github.com/ros2/rosidl/pull/432 (Foxy)
#                    and https://github.com/ros2/rosidl/pull/538
message_c_include_prefix = idl_structure_type_to_c_include_prefix(message.structure.namespaced_type)
# Strip off any service or action suffix
if message_c_include_prefix.endswith('__request'):
    message_c_include_prefix = message_c_include_prefix[:-9]
elif message_c_include_prefix.endswith('__response'):
    message_c_include_prefix = message_c_include_prefix[:-10]
if message_c_include_prefix.endswith('__goal'):
    message_c_include_prefix = message_c_include_prefix[:-6]
elif message_c_include_prefix.endswith('__result'):
    message_c_include_prefix = message_c_include_prefix[:-8]
elif message_c_include_prefix.endswith('__feedback'):
    message_c_include_prefix = message_c_include_prefix[:-10]
elif message_c_include_prefix.endswith('__send_goal'):
    message_c_include_prefix = message_c_include_prefix[:-11]
elif message_c_include_prefix.endswith('__get_result'):
    message_c_include_prefix = message_c_include_prefix[:-12]
}@

#include <jni.h>

#include <cassert>
#include <cstdint>
#include <string>

#include "rosidl_runtime_c/message_type_support_struct.h"

#include "rcljava_common/exceptions.hpp"
#include "rcljava_common/signatures.hpp"

@[for include in member_includes]@
#include "@(include)"
@[end for]@

#include "@(message_c_include_prefix).h"

// Ensure that a jlong is big enough to store raw pointers
static_assert(sizeof(jlong) >= sizeof(std::intptr_t), "jlong must be able to store pointers");

using rcljava_common::exceptions::rcljava_throw_exception;

#ifdef __cplusplus
extern "C" {
#endif

// Initialize cached types in the anonymous namespace to avoid linking conflicts
namespace
{
JavaVM * g_vm = nullptr;

@[for normalized_type, jni_type in cache.items()]@
jclass _j@(normalized_type)_class_global = nullptr;
@[ if constructor_signatures[jni_type]]@
jmethodID _j@(normalized_type)_constructor_global = nullptr;
@[ end if]@

@[ if value_methods.get(jni_type)]@
jmethodID _j@(normalized_type)_value_global = nullptr;
@[ end if]@

@[ if jni_type in namespaced_types]@
jmethodID _j@(normalized_type)_from_java_converter_global = nullptr;
using _j@(normalized_type)_from_java_signature = @(normalized_type) * (*)(jobject, @(normalized_type) *);
jlong _j@(normalized_type)_from_java_converter_ptr_global = 0;
_j@(normalized_type)_from_java_signature _j@(normalized_type)_from_java_function = nullptr;

jmethodID _j@(normalized_type)_to_java_converter_global = nullptr;
using _j@(normalized_type)_to_java_signature = jobject (*)(@(normalized_type) *, jobject);
jlong _j@(normalized_type)_to_java_converter_ptr_global = 0;
_j@(normalized_type)_to_java_signature _j@(normalized_type)_to_java_function = nullptr;
@[ end if]@
@[end for]@
}  // namespace

@{
from rosidl_generator_java import get_jni_mangled_name

message_fqn = message.structure.namespaced_type.namespaced_name()
underscore_separated_type_name = '_'.join(message_fqn)
underscore_separated_jni_type_name = get_jni_mangled_name(message_fqn)
}@
/*
 * Class:     @(underscore_separated_type_name)
 * Method:    getFromJavaConverter
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_@(underscore_separated_jni_type_name)_getFromJavaConverter
  (JNIEnv *, jclass);

/*
 * Class:     @(underscore_separated_type_name)
 * Method:    getToJavaConverter
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_@(underscore_separated_jni_type_name)_getToJavaConverter
  (JNIEnv *, jclass);

/*
 * Class:     @(underscore_separated_type_name)
 * Method:    getTypeSupport
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_@(underscore_separated_jni_type_name)_getTypeSupport
  (JNIEnv *, jclass);

/*
 * Class:     @(underscore_separated_type_name)
 * Method:    getDestructor
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_@(underscore_separated_jni_type_name)_getDestructor
  (JNIEnv *, jclass);

#ifdef __cplusplus
}
#endif

@# Avoid warnings about unused arguments if the message definition does not contain any members
@[if message.structure.members]@
@(msg_normalized_type) * @(underscore_separated_type_name)__convert_from_java(jobject _jmessage_obj, @(msg_normalized_type) * ros_message)
@[else]@
@(msg_normalized_type) * @(underscore_separated_type_name)__convert_from_java(jobject, @(msg_normalized_type) * ros_message)
@[end if]@
{
  JNIEnv * env = nullptr;
  // TODO(esteve): check return status
  assert(g_vm != nullptr);
  g_vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION_1_6);
  assert(env != nullptr);

  if (ros_message == nullptr) {
    ros_message = @(msg_normalized_type)__create();
  }
@[for member in message.structure.members]@
@{
normalized_type = get_normalized_type(member.type)
}@
  // BEGIN NEW MEMBER: @(member.name)
@[  if isinstance(member.type, AbstractNestedType)]@
  // AbstractNestedType
@[    if isinstance(member.type.value_type, BasicType)]@
  // Special optimization for BasicType
  auto _jfield_@(member.name)_fid = env->GetFieldID(_j@(msg_normalized_type)_class_global, "@(member.name)", "[@(get_jni_primitive_signature(member.type.value_type))");
@[    else]@
  // Original unoptimized
  auto _jfield_@(member.name)_fid = env->GetFieldID(_j@(msg_normalized_type)_class_global, "@(member.name)", "L@(list_jni_type);");
@[    end if]@
  jobject _jlist_@(member.name)_object = env->GetObjectField(_jmessage_obj, _jfield_@(member.name)_fid);

  if (_jlist_@(member.name)_object != nullptr) {

@[    if isinstance(member.type.value_type, BasicType)]@
    // BasicType
    // This is a basic type, so construct a native array pointer for access
    @(get_jni_array_cpp_type(member.type.value_type)) _jlist_@(member.name)_jarray = reinterpret_cast<j@(get_java_type(member.type.value_type, use_primitives=True))Array>(_jlist_@(member.name)_object);
@[    end if]@

@[    if isinstance(member.type, AbstractSequence)]@
    // AbstractSequence
    // Non-constant size array, need to get size by function

@[      if isinstance(member.type.value_type, BasicType)]@
    // Optimized path for BasicTypes
    jint _jlist_@(member.name)_size = env->GetArrayLength(_jlist_@(member.name)_jarray);
@[      else]@
    // Unoptimized path
    jmethodID _jlist_@(member.name)_size_mid = env->GetMethodID(_j@(list_normalized_type)_class_global, "size", "()I");
    jint _jlist_@(member.name)_size = env->CallIntMethod(_jlist_@(member.name)_object, _jlist_@(member.name)_size_mid);
@[      end if]@

@[      if isinstance(member.type.value_type, AbstractString)]@
    if (!rosidl_runtime_c__String__Sequence__init(&(ros_message->@(member.name)), _jlist_@(member.name)_size)) {
      rcljava_throw_exception(env, "java/lang/IllegalStateException", "unable to create String__Array ros_message");
    }
@[      elif isinstance(member.type.value_type, AbstractWString)]@
    if (!rosidl_runtime_c__U16String__Sequence__init(&(ros_message->@(member.name)), _jlist_@(member.name)_size)) {
      rcljava_throw_exception(env, "java/lang/IllegalStateException", "unable to create U16String__Array ros_message");
    }
@[      elif isinstance(member.type.value_type, BasicType)]@
    if (!rosidl_runtime_c__@(member.type.value_type.typename)__Sequence__init(&(ros_message->@(member.name)), _jlist_@(member.name)_size)) {
      rcljava_throw_exception(env, "java/lang/IllegalStateException", "unable to create @(member.type.value_type)__Array ros_message");
    }
@[      else]@
    if (!@('__'.join(member.type.value_type.namespaced_name()))__Sequence__init(&(ros_message->@(member.name)), _jlist_@(member.name)_size)) {
      rcljava_throw_exception(env, "java/lang/IllegalStateException", "unable to create @(member.type.value_type)__Array ros_message");
    }    
@[      end if]@

    auto _dest_@(member.name) = ros_message->@(member.name).data;
@# Not an AbstractSequence
@[    else]@
    // !AbstractSequence
    // Constant size array, size is known at runtime
    jint _jlist_@(member.name)_size = @(member.type.size);

    auto _dest_@(member.name) = ros_message->@(member.name);
@# AbstractSequence
@[    end if]@
@[    if isinstance(member.type.value_type, BasicType)]@
    // BasicType
    // Copy the native array directly to the destination buffer in a single call
    env->@(get_jni_array_func(member.type.value_type, 3))(_jlist_@(member.name)_jarray, 0, _jlist_@(member.name)_size, reinterpret_cast<j@(get_java_type(member.type.value_type, use_primitives=True))*>(_dest_@(member.name)));
@[    else]@
    // !BasicType
    // "getter" method
    jmethodID _jlist_@(member.name)_get_mid = env->GetMethodID(_j@(list_normalized_type)_class_global, "get", "(I)Ljava/lang/Object;");

    // This is not a basic type, so we must copy it one-element-at-a-time
    for (jint i = 0; i < _jlist_@(member.name)_size; ++i) {
      
      // Call the List::get() function to get an element from the list
      auto element = env->CallObjectMethod(_jlist_@(member.name)_object, _jlist_@(member.name)_get_mid, i);
@[      if isinstance(member.type.value_type, AbstractString)]@
      jstring _jfield_@(member.name)_value = static_cast<jstring>(element);
      if (_jfield_@(member.name)_value != nullptr) {
        const char * _str@(member.name) = env->GetStringUTFChars(_jfield_@(member.name)_value, 0);
        rosidl_runtime_c__String__assign(
          &_dest_@(member.name)[i], _str@(member.name));
        env->ReleaseStringUTFChars(_jfield_@(member.name)_value, _str@(member.name));
      }
@[      elif isinstance(member.type.value_type, AbstractWString)]@
      jstring _jfield_@(member.name)_value = static_cast<jstring>(element);
      if (_jfield_@(member.name)_value != nullptr) {
        const jchar * _str@(member.name) = env->GetStringChars(_jfield_@(member.name)_value, 0);
        rosidl_runtime_c__U16String__assign(
          &_dest_@(member.name)[i], _str@(member.name));
        env->ReleaseStringChars(_jfield_@(member.name)_value, _str@(member.name));
      }
@[      else]@
      // Element is itself a complex / user-defined type. Call the respective _from_java_function()
      _dest_@(member.name)[i] = *_j@(normalized_type)_from_java_function(element, nullptr);
@# End of type-based specialization
@[    end if]@
      // Free the ref to the local element, no matter what we did with it
      env->DeleteLocalRef(element);
    }
@# End of optimization for arrays of basic-types
@[  end if]@
  } else {
    rcljava_throw_exception(env, "java/lang/IllegalStateException", "unable to get @(member.name) object field");
  }
@[  else]@
  // !AbstractNestedType
@[    if isinstance(member.type, AbstractGenericString)]@
  auto _jfield_@(member.name)_fid = env->GetFieldID(_j@(msg_normalized_type)_class_global, "@(member.name)", "Ljava/lang/String;");
  jstring _jvalue@(member.name) = static_cast<jstring>(env->GetObjectField(_jmessage_obj, _jfield_@(member.name)_fid));

  if (_jvalue@(member.name) != nullptr) {
@[      if isinstance(member.type, AbstractString)]@
    const char * _str@(member.name) = env->GetStringUTFChars(_jvalue@(member.name), 0);
    rosidl_runtime_c__String__assign(
      &ros_message->@(member.name), _str@(member.name));
    env->ReleaseStringUTFChars(_jvalue@(member.name), _str@(member.name));
@[      else]@
    const jchar * _str@(member.name) = env->GetStringChars(_jvalue@(member.name), 0);
    rosidl_runtime_c__U16String__assign(
      &ros_message->@(member.name), _str@(member.name));
    env->ReleaseStringChars(_jvalue@(member.name), _str@(member.name));
@[      end if]@
  }
@[    elif isinstance(member.type, BasicType)]@
@{
jni_signature = get_jni_signature(member.type)
get_method_name = 'Get%sField' % get_java_type(member.type, use_primitives=True).capitalize()
}@
  auto _jfield_@(member.name)_fid = env->GetFieldID(_j@(msg_normalized_type)_class_global, "@(member.name)", "@(jni_signature)");
  ros_message->@(member.name) = env->@(get_method_name)(_jmessage_obj, _jfield_@(member.name)_fid);
@[    else]@
  auto _jfield_@(member.name)_fid = env->GetFieldID(
    _j@(msg_normalized_type)_class_global, "@(member.name)", "L@('/'.join(member.type.namespaced_name()));");
  assert(_jfield_@(member.name)_fid != nullptr);

  jobject _jfield_@(member.name)_obj = env->GetObjectField(_jmessage_obj, _jfield_@(member.name)_fid);

  if (_jfield_@(member.name)_obj != nullptr) {
    ros_message->@(member.name) = *_j@(normalized_type)_from_java_function(_jfield_@(member.name)_obj, nullptr);
  }
  env->DeleteLocalRef(_jfield_@(member.name)_obj);
@[    end if]@
@[  end if]@
  // END NEW MEMBER @(member.name)

@[end for]@
  assert(ros_message != nullptr);
  return ros_message;
}

@# Avoid warnings about unused arguments if the message definition does not contain any fields
@[if message.structure.members]@
jobject @(underscore_separated_type_name)__convert_to_java(@(msg_normalized_type) * _ros_message, jobject _jmessage_obj)
@[else]@
jobject @(underscore_separated_type_name)__convert_to_java(@(msg_normalized_type) *, jobject _jmessage_obj)
@[end if]@
{
  JNIEnv * env = nullptr;
  // TODO(esteve): check return status
  assert(g_vm != nullptr);
  g_vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION_1_6);
  assert(env != nullptr);

  if (_jmessage_obj == nullptr) {
    _jmessage_obj = env->NewObject(_j@(msg_normalized_type)_class_global, _j@(msg_normalized_type)_constructor_global);
  }
@[for member in message.structure.members]@
@{
normalized_type = get_normalized_type(member.type)
}@
  // BEGIN NEW MEMBER: @(member.name)
@[  if isinstance(member.type, AbstractNestedType)]@
  // AbstractNestedType
@[    if isinstance(member.type.value_type, BasicType)]@
  // BasicType
  auto _jfield_@(member.name)_fid = env->GetFieldID(_j@(msg_normalized_type)_class_global, "@(member.name)", "[@(get_jni_primitive_signature(member.type.value_type))");
  @[    if isinstance(member.type, Array)]@
  auto _jfield_@(member.name)_array_size = @(member.type.size);
@[      else]@
  auto _jfield_@(member.name)_array_size = _ros_message->@(member.name).size;
@[      end if]@
  auto _jarray_@(member.name)_array = env->@(get_jni_array_func(member.type.value_type, 0))(_jfield_@(member.name)_array_size);
  jobject _jarray_list_@(member.name)_obj = reinterpret_cast<jobject>(_jarray_@(member.name)_array);
@[    elif isinstance(member.type.value_type, AbstractGenericString)]@
  // AbstractGenericString
  auto _jfield_@(member.name)_fid = env->GetFieldID(_j@(msg_normalized_type)_class_global, "@(member.name)", "L@(list_jni_type);");
  jobject _jarray_list_@(member.name)_obj = env->NewObject(_j@(array_list_normalized_type)_class_global, _j@(array_list_normalized_type)_constructor_global);

  // TODO(esteve): replace ArrayList with a jobjectArray to initialize the array beforehand
  jmethodID _jlist_@(member.name)_add_mid = env->GetMethodID(_j@(array_list_normalized_type)_class_global, "add", "(Ljava/lang/Object;)Z");

@[      if isinstance(member.type, Array)]@

  for (size_t i = 0; i < @(member.type.size); ++i) {
    auto _ros_@(member.name)_element = _ros_message->@(member.name)[i];

@[      else]@

  for (size_t i = 0; i < _ros_message->@(member.name).size; ++i) {
    auto _ros_@(member.name)_element = _ros_message->@(member.name).data[i];

@[      end if]@

@[      if isinstance(member.type.value_type, AbstractGenericString)]@
    // AbstractGenericString
    jobject _jlist_@(member.name)_element = nullptr;
    if (_ros_@(member.name)_element.data != nullptr) {
@[        if isinstance(member.type.value_type, AbstractString)]@
      _jlist_@(member.name)_element = env->NewStringUTF(_ros_@(member.name)_element.data);
@[        else]@
      _jlist_@(member.name)_element = env->NewString(_ros_@(member.name)_element.data, _ros_@(member.name)_element.size);
@[        end if]@
    }
@[      else]@

    jobject _jlist_@(member.name)_element = env->NewObject(
      _j@(normalized_type)_class_global, _j@(normalized_type)_constructor_global, _ros_@(member.name)_element);

@[      end if]@
    if (_jlist_@(member.name)_element != nullptr) {
      jboolean _jlist_@(member.name)_add_result = env->CallBooleanMethod(_jarray_list_@(member.name)_obj, _jlist_@(member.name)_add_mid, _jlist_@(member.name)_element);
      assert(_jlist_@(member.name)_add_result);
    }
  }
@# !BasicType, !AbstractGenericString
@[    else]@
  // !BasicType, !AbstractGenericString
  auto _jfield_@(member.name)_fid = env->GetFieldID(_j@(msg_normalized_type)_class_global, "@(member.name)", "L@(list_jni_type);");
  jobject _jarray_list_@(member.name)_obj = env->NewObject(_j@(array_list_normalized_type)_class_global, _j@(array_list_normalized_type)_constructor_global);
  // TODO(esteve): replace ArrayList with a jobjectArray to initialize the array beforehand
  jmethodID _jlist_@(member.name)_add_mid = env->GetMethodID(_j@(array_list_normalized_type)_class_global, "add", "(Ljava/lang/Object;)Z");
@[      if isinstance(member.type, Array)]@
  for (size_t i = 0; i < @(member.type.size); ++i) {
    jobject _jlist_@(member.name)_element = _j@(normalized_type)_to_java_function(&(_ros_message->@(member.name)[i]), nullptr);
@[      else]@
  for (size_t i = 0; i < _ros_message->@(member.name).size; ++i) {
    jobject _jlist_@(member.name)_element = _j@(normalized_type)_to_java_function(&(_ros_message->@(member.name).data[i]), nullptr);
@[      end if]@
    if (_jlist_@(member.name)_element != nullptr) {
      jboolean _jlist_@(member.name)_add_result = env->CallBooleanMethod(_jarray_list_@(member.name)_obj, _jlist_@(member.name)_add_mid, _jlist_@(member.name)_element);
      assert(_jlist_@(member.name)_add_result);
    }
  }
@[    end if]@
  env->SetObjectField(_jmessage_obj, _jfield_@(member.name)_fid, _jarray_list_@(member.name)_obj);
  env->DeleteLocalRef(_jarray_list_@(member.name)_obj);
@[  else]@
  // !AbstractNestedType
@[    if isinstance(member.type, AbstractGenericString)]@
  auto _jfield_@(member.name)_fid = env->GetFieldID(_j@(msg_normalized_type)_class_global, "@(member.name)", "Ljava/lang/String;");
  if (_ros_message->@(member.name).data != nullptr) {
@[      if isinstance(member.type, AbstractString)]@
    env->SetObjectField(_jmessage_obj, _jfield_@(member.name)_fid, env->NewStringUTF(_ros_message->@(member.name).data));
@[      else]@
    env->SetObjectField(_jmessage_obj, _jfield_@(member.name)_fid, env->NewString(_ros_message->@(member.name).data, _ros_message->@(member.name).size));
@[      end if]@
  }
@[    elif isinstance(member.type, BasicType)]@
@{
jni_signature = get_jni_signature(member.type)
set_method_name = 'Set%sField' % get_java_type(member.type, use_primitives=True).capitalize()
}@
  auto _jfield_@(member.name)_fid = env->GetFieldID(_j@(msg_normalized_type)_class_global, "@(member.name)", "[@(jni_signature)");
  env->@(set_method_name)(_jmessage_obj, _jfield_@(member.name)_fid, _ros_message->@(member.name));
@[    else]@
  auto _jfield_@(member.name)_fid = env->GetFieldID(
    _j@(msg_normalized_type)_class_global, "@(member.name)", "L@('/'.join(member.type.namespaced_name()));");
  assert(_jfield_@(member.name)_fid != nullptr);

  jobject _jfield_@(member.name)_obj = _j@(normalized_type)_to_java_function(&(_ros_message->@(member.name)), nullptr);

  env->SetObjectField(_jmessage_obj, _jfield_@(member.name)_fid, _jfield_@(member.name)_obj);
@[    end if]@
@[  end if]@
// END NEW MEMBER: @(member.name)

@[end for]@
  assert(_jmessage_obj != nullptr);
  return _jmessage_obj;
}

JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM * vm, void *)
{
  // Can only call this once
  if (g_vm == nullptr) {
    g_vm = vm;
  }

  JNIEnv * env;
  if (g_vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION_1_6) != JNI_OK) {
    return JNI_ERR;
  } else {
@[for normalized_type, jni_type in cache.items()]@
    auto _j@(normalized_type)_class_local = env->FindClass("@(jni_type)");
    assert(_j@(normalized_type)_class_local != nullptr);
    _j@(normalized_type)_class_global = static_cast<jclass>(env->NewGlobalRef(_j@(normalized_type)_class_local));
    env->DeleteLocalRef(_j@(normalized_type)_class_local);
    assert(_j@(normalized_type)_class_global != nullptr);
@[  if constructor_signatures[jni_type]]@
    _j@(normalized_type)_constructor_global = env->GetMethodID(_j@(normalized_type)_class_global, "<init>", "@(constructor_signatures[jni_type])");
    assert(_j@(normalized_type)_constructor_global != nullptr);
@[  end if]@
@{
value_method = value_methods.get(jni_type)
if value_method:
    value_method_name, value_method_signature = value_method
}@
@[  if value_method]@
    _j@(normalized_type)_value_global = env->GetMethodID(_j@(normalized_type)_class_global, "@(value_method_name)", "@(value_method_signature)");
    assert(_j@(normalized_type)_value_global != nullptr);
@[  end if]@
@[  if jni_type in namespaced_types]@
    _j@(normalized_type)_from_java_converter_global = env->GetStaticMethodID(
      _j@(normalized_type)_class_global, "getFromJavaConverter", "()J");
    assert(_j@(normalized_type)_from_java_converter_global != nullptr);

    _j@(normalized_type)_from_java_converter_ptr_global = env->CallStaticLongMethod(
      _j@(normalized_type)_class_global, _j@(normalized_type)_from_java_converter_global);
    assert(_j@(normalized_type)_from_java_converter_ptr_global != 0);

    _j@(normalized_type)_from_java_function =
      reinterpret_cast<_j@(normalized_type)_from_java_signature>(_j@(normalized_type)_from_java_converter_ptr_global);
    assert(_j@(normalized_type)_from_java_function != nullptr);

    _j@(normalized_type)_to_java_converter_global = env->GetStaticMethodID(
      _j@(normalized_type)_class_global, "getToJavaConverter", "()J");
    assert(_j@(normalized_type)_to_java_converter_global != nullptr);

    _j@(normalized_type)_to_java_converter_ptr_global = env->CallStaticLongMethod(
      _j@(normalized_type)_class_global, _j@(normalized_type)_to_java_converter_global);
    assert(_j@(normalized_type)_to_java_converter_ptr_global != 0);

    _j@(normalized_type)_to_java_function =
      reinterpret_cast<_j@(normalized_type)_to_java_signature>(_j@(normalized_type)_to_java_converter_ptr_global);
    assert(_j@(normalized_type)_to_java_function != nullptr);
@[  end if]@
@[end for]@
  }
  return JNI_VERSION_1_6;
}

JNIEXPORT void JNICALL JNI_OnUnload(JavaVM * vm, void *)
{
  assert(g_vm != nullptr);
  assert(g_vm == vm);

  JNIEnv * env;
  if (g_vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION_1_6) == JNI_OK) {
@[for normalized_type, jni_type in cache.items()]@
    if (_j@(normalized_type)_class_global != nullptr) {
      env->DeleteGlobalRef(_j@(normalized_type)_class_global);
      _j@(normalized_type)_class_global = nullptr;
@[  if constructor_signatures[jni_type]]@
      _j@(normalized_type)_constructor_global = nullptr;
@[  end if]@
@[  if value_methods.get(jni_type)]@
      _j@(normalized_type)_value_global = nullptr;
@[  end if]@
@[  if jni_type in namespaced_types]@
      _j@(normalized_type)_from_java_converter_global = nullptr;
      _j@(normalized_type)_from_java_converter_ptr_global = 0;
      _j@(normalized_type)_from_java_function = nullptr;

      _j@(normalized_type)_to_java_converter_global = nullptr;
      _j@(normalized_type)_to_java_converter_ptr_global = 0;
      _j@(normalized_type)_to_java_function = nullptr;
@[  end if]@
    }
@[end for]@
  }
}

JNIEXPORT jlong JNICALL Java_@(underscore_separated_jni_type_name)_getFromJavaConverter(JNIEnv *, jclass)
{
  jlong ptr = reinterpret_cast<jlong>(&@(underscore_separated_type_name)__convert_from_java);
  return ptr;
}

JNIEXPORT jlong JNICALL Java_@(underscore_separated_jni_type_name)_getToJavaConverter(JNIEnv *, jclass)
{
  jlong ptr = reinterpret_cast<jlong>(@(underscore_separated_type_name)__convert_to_java);
  return ptr;
}

JNIEXPORT jlong JNICALL Java_@(underscore_separated_jni_type_name)_getTypeSupport(JNIEnv *, jclass)
{
  jlong ptr = reinterpret_cast<jlong>(ROSIDL_GET_MSG_TYPE_SUPPORT(@(','.join(message.structure.namespaced_type.namespaced_name()))));
  return ptr;
}

JNIEXPORT jlong JNICALL Java_@(underscore_separated_jni_type_name)_getDestructor(JNIEnv *, jclass)
{
  jlong ptr = reinterpret_cast<jlong>(@(msg_normalized_type)__destroy);
  return ptr;
}
