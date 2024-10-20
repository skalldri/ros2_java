cmake_minimum_required(VERSION 3.5)

project(termium_java)

find_package(ament_cmake REQUIRED)

include(FetchContent)

# Fetch OpenJDK 11, the last version with support for Java 6
FetchContent_Declare(
    Termium
    URL https://api.adoptium.net/v3/binary/latest/11/ga/linux/x64/jdk/hotspot/normal/eclipse
    DOWNLOAD_EXTRACT_TIMESTAMP false
)

FetchContent_GetProperties(Termium)

FetchContent_MakeAvailable(Termium)

# Ensure the ./bin directory is executable
file(CHMOD ${termium_SOURCE_DIR}/bin/ FILE_PERMISSIONS OWNER_EXECUTE)

# Install all files, with matching permissions. This is critical!
install(DIRECTORY ${termium_SOURCE_DIR}/ DESTINATION java_home USE_SOURCE_PERMISSIONS)

ament_package(CONFIG_EXTRAS termium_java-extra.cmake)