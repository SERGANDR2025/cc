cmake_minimum_required(VERSION 3.15)
cmake_policy(SET CMP0167 NEW)

project(CryptoWebSocket CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Задаём каталоги для выходных файлов
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")

# Настройки оптимизации
if(CMAKE_CXX_COMPILER_ID MATCHES "MSVC")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /Od /Zi")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /O2 /arch:AVX2")
else()
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -march=native -mavx2")
endif()

# Поиск зависимостей
find_package(websocketpp CONFIG REQUIRED)
find_package(Boost REQUIRED COMPONENTS system random container)  # 🔹 Добавлен Boost.Container
find_package(simdjson CONFIG REQUIRED)
find_package(TBB REQUIRED)

# Создаём исполняемый файл
add_executable(CryptoWebSocket main.cpp)

# Линковка для всех конфигураций
target_link_libraries(CryptoWebSocket PRIVATE 
    websocketpp::websocketpp 
    Boost::system 
    Boost::random 
    Boost::container  # 🔹 Линкуем Boost.Container для CircularBuffer
    simdjson::simdjson 
    TBB::tbb
)
