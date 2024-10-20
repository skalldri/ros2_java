
include(FetchContent)

FetchContent_Declare(
    Termium
    URL https://api.adoptium.net/v3/binary/latest/11/ga/linux/x64/jdk/hotspot/normal/eclipse
    DOWNLOAD_EXTRACT_TIMESTAMP false
)

FetchContent_GetProperties(Termium)

FetchContent_MakeAvailable(Termium)

set(Termium_FOUND true)