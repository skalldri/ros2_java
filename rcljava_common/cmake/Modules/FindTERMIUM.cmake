# Copyright 2024 Stuart Alldritt <s.k.alldritt@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
include(FindPackageHandleStandardArgs)
include(FetchContent)

find_package_handle_standard_args(TERMIUM)

fetchcontent_declare(
    Termium
    URL https://api.adoptium.net/v3/binary/latest/11/ga/linux/x64/jdk/hotspot/normal/eclipse
    DOWNLOAD_EXTRACT_TIMESTAMP false
)

fetchcontent_getproperties(Termium)

fetchcontent_makeavailable(Termium)

set(Termium_FOUND true)