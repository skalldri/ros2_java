cmake_minimum_required(VERSION 3.5)
project(rosidl_generator_java_tests)

find_package(ament_cmake REQUIRED)
find_package(rcljava_common REQUIRED)

include(CrossCompilingExtra)

# Find the special "termium_java" package, which will
# download a specific version of Java and override JAVA_HOME to let us find it
find_package(termium_java REQUIRED)

if(CMAKE_CROSSCOMPILING)
  find_host_package(Java COMPONENTS Development REQUIRED)
else()
  find_package(Java COMPONENTS Development REQUIRED)
endif()
if(NOT ANDROID)
  find_package(JNI REQUIRED)
endif()
include(UseJava)
include(JavaExtra)

if(BUILD_TESTING)
  find_package(ament_cmake_gtest REQUIRED)
  find_package(ament_cmake_pytest REQUIRED)
  find_package(rosidl_cmake REQUIRED)
  find_package(rosidl_generator_cpp REQUIRED)
  find_package(rosidl_generator_type_description REQUIRED)
  find_package(rosidl_runtime_c REQUIRED)
  find_package(rosidl_runtime_cpp REQUIRED)
  find_package(test_interface_files REQUIRED)
  find_package(type_description_interfaces REQUIRED)

  # Disable flake8 linter
  # TODO(jacobperron): Re-enable after https://github.com/ament/ament_lint/pull/252
  set(ament_cmake_flake8_FOUND TRUE)

  find_package(ament_lint_auto REQUIRED)
  ament_lint_auto_find_test_dependencies()

  # TODO(jacobperron): This fixes generating messages with wstrings.
  # Figure out why this is needed as of Foxy, and how we can remove this workaround
  set(CMAKE_JAVA_COMPILE_FLAGS "-encoding" "ISO-8859-1")

  rosidl_generate_interfaces(${PROJECT_NAME}
    ${test_interface_files_ACTION_FILES}
    ${test_interface_files_MSG_FILES}
    ${test_interface_files_SRV_FILES}
    ADD_LINTER_TESTS
    SKIP_INSTALL
  )

  set(${PROJECT_NAME}_test_sources
    "src/test/java/org/ros2/generator/InterfacesTest.java"
  )

  set(${PROJECT_NAME}_testsuites
    "org.ros2.generator.InterfacesTest"
  )

  get_property(_${PROJECT_NAME}_messages_jar_file
    TARGET "${PROJECT_NAME}_messages_jar"
    PROPERTY "JAR_FILE")

  set(_deps_library_dirs "")
  list_append_unique(_deps_library_dirs ${CMAKE_CURRENT_BINARY_DIR})
  list_append_unique(_deps_library_dirs ${CMAKE_CURRENT_BINARY_DIR}/rosidl_generator_java/rosidl_generator_java_tests/msg/)
  list_append_unique(_deps_library_dirs ${CMAKE_CURRENT_BINARY_DIR}/rosidl_generator_java/rosidl_generator_java_tests/srv/)

  foreach(testsuite ${${PROJECT_NAME}_testsuites})
    ament_add_junit_tests("${PROJECT_NAME}_tests_${testsuite}"
      TIMEOUT 90
      WORKING_DIRECTORY
      "${CMAKE_CURRENT_BINARY_DIR}"
      SOURCES
      "${${PROJECT_NAME}_test_sources}"
      TESTS
      "${testsuite}"
      INCLUDE_JARS
      "${rcljava_common_JARS}"
      "${rosidl_generator_java_JARS}"
      "${_${PROJECT_NAME}_messages_jar_file}"
      APPEND_LIBRARY_DIRS
      "${_deps_library_dirs}"
    )

    add_dependencies("${PROJECT_NAME}_tests_${testsuite}" "${PROJECT_NAME}_messages_jar")
  endforeach()

endif()

ament_package()
