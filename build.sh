#!/bin/bash
# Cross-build the ROS 2 Java/Android bindings (rclandroid AAR) for the Segway Loomo tablet:
# Android 5.1 (API 22) on an Intel Atom x86_64.
#
# This script is the single build recipe; CI (.github/workflows/android.yml) and the robot
# configuration repo (loomo_jetson_config/scripts/build-rclandroid.sh) both call it.
#
# Expects to live at <workspace>/src/ros2-java/ros2_java/build.sh, i.e. after
#   vcs import --input ros2_java_android.repos src
# Required environment:
#   ANDROID_HOME   Android SDK with build-tools;34.0.0, platform-tools, platforms;android-22
#   ANDROID_NDK    NDK 26.3.11579264 (default: $ANDROID_HOME/ndk/26.3.11579264)
#   JAVA_HOME      JDK 17 (javac -source 1.6 is handled by the bundled termium_java JDK 11)
#   gradle 8.6..8.x on PATH or GRADLE_HOME (AGP 8.4 needs >= 8.6; Gradle 9 removes jcenter())
#   colcon, vcstool, colcon-gradle, colcon-ros-gradle, and the rosidl generator imports (empy 3.3.4,
#   lark, numpy, pyyaml, catkin_pkg) importable by the python3 on PATH.
# No host ROS install is needed: the workspace cross-builds every ROS package itself.
# Optional: WS (workspace root), ANDROID_ABI (x86_64), ANDROID_NATIVE_API_LEVEL (android-22),
#   ANDROID_STL (c++_shared), extra colcon args are passed through ("$@").
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WS="${WS:-$(cd "$HERE/../../.." && pwd)}"
: "${ANDROID_HOME:?set ANDROID_HOME to the Android SDK root}"
export ANDROID_SDK_ROOT="${ANDROID_SDK_ROOT:-$ANDROID_HOME}"
export ANDROID_NDK="${ANDROID_NDK:-$ANDROID_HOME/ndk/26.3.11579264}"
export ANDROID_ABI="${ANDROID_ABI:-x86_64}"
export ANDROID_NATIVE_API_LEVEL="${ANDROID_NATIVE_API_LEVEL:-android-22}"
export ANDROID_STL="${ANDROID_STL:-c++_shared}"
[[ -f "$ANDROID_NDK/build/cmake/android.toolchain.cmake" ]] || { echo "NDK not found at $ANDROID_NDK" >&2; exit 1; }

# rclandroid/build.gradle still uses jcenter(), which Gradle 9 removed; AGP 8.4 needs >= 8.6.
GRADLE_BIN="${GRADLE_HOME:+$GRADLE_HOME/bin/}gradle"
GRADLE_VER="$("$GRADLE_BIN" --version 2>/dev/null | sed -n 's/^Gradle \([0-9.]*\).*/\1/p' | head -1)"
case "$GRADLE_VER" in
  8.[6-9]*|8.[1-9][0-9]*) ;;
  *) echo "Need Gradle 8.6..8.x (found '${GRADLE_VER:-none}' via ${GRADLE_BIN}); set GRADLE_HOME" >&2; exit 1 ;;
esac

# Host python that runs the rosidl generators. distutils is gone in Python 3.12; use sysconfig.
PYTHON3_EXEC="$(command -v python3)"
PYTHON3_LIBRARY="$("$PYTHON3_EXEC" -c 'import os, sysconfig; print(os.path.realpath(os.path.join(sysconfig.get_config_var("LIBPL"), sysconfig.get_config_var("LDLIBRARY"))))')"
PYTHON3_INCLUDE_DIR="$("$PYTHON3_EXEC" -c 'import sysconfig; print(sysconfig.get_config_var("INCLUDEPY"))')"

# colcon prefers its PowerShell shell extension when `pwsh` exists (GitHub runners have it). That
# extension mangles the editable-install hook for python packages: PYTHONPATH ends up as
# "<prefix>//abs/path/build/ament_package" and ament_cmake_core then cannot import ament_package.
# Stay on the POSIX shell extensions.
export COLCON_EXTENSION_BLOCKLIST="${COLCON_EXTENSION_BLOCKLIST:-colcon_core.shell.powershell}"

cd "$WS"
echo "Workspace: $WS"
echo "NDK: $ANDROID_NDK  ABI: $ANDROID_ABI  API: $ANDROID_NATIVE_API_LEVEL  STL: $ANDROID_STL"

colcon build \
  --metas "$HERE/colcon.android.meta" \
  --symlink-install \
  --event-handlers console_cohesion+ \
  --packages-up-to rclandroid \
  --packages-ignore uncrustify_vendor rosidl_generator_py test_ros2trace test_launch_testing lttngpy \
  --gradle-args \
    --info -Pament.android_stl="${ANDROID_STL}" -Pament.android_abi="${ANDROID_ABI}" -Pament.android_ndk="${ANDROID_NDK}" -Pament.android_variant=release \
  --cmake-args \
    -C "$HERE/TryRunResults-Loomo-Android5.1.1.cmake" \
    -DCMAKE_TOOLCHAIN_FILE="${ANDROID_NDK}/build/cmake/android.toolchain.cmake" \
    -DANDROID_FUNCTION_LEVEL_LINKING=OFF \
    -DANDROID_NATIVE_API_LEVEL="${ANDROID_NATIVE_API_LEVEL}" \
    -DANDROID_ABI="${ANDROID_ABI}" \
    -DANDROID_NDK="${ANDROID_NDK}" \
    -DANDROID_STL="${ANDROID_STL}" \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_TESTING=OFF \
    -DCMAKE_FIND_ROOT_PATH="${WS}/install" \
    -DPython3_EXECUTABLE="${PYTHON3_EXEC}" \
    -DPython3_LIBRARY="${PYTHON3_LIBRARY}" \
    -DPython3_INCLUDE_DIR="${PYTHON3_INCLUDE_DIR}" \
  "$@"

AAR="${WS}/install/rclandroid/share/rclandroid/android/rclandroid-release.aar"
ls -la "$AAR"
echo "AAR=$AAR"
