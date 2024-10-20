
include(FetchContent)

FetchContent_Declare(
    Termium
    URL https://api.adoptium.net/v3/binary/latest/17/ga/linux/x64/jdk/hotspot/normal/eclipse
    DOWNLOAD_EXTRACT_TIMESTAMP false
)

FetchContent_GetProperties(Termium)

FetchContent_MakeAvailable(Termium)

message(WARNING "Hello my name is ${termium_SOURCE_DIR}")

set(Termium_FOUND true)