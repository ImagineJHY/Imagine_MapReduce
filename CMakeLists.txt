cmake_minimum_required(VERSION 3.0)
project(IMAGINE_MAPREDUCE)

# set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g  -std=c++11 -Wall -Werror")
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -std=c++11 -Wall -Werror")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g  -std=c++11")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -std=c++11")

string(ASCII 27 Esc)
set(R "${Esc}[0;31m")
set(B "${Esc}[1;34m")
set(E "${Esc}[m")

message("${B}[Constructing LIB][Imagine MapReduce]: Starting Imagine_MapReduce lib building, lib name is ${PROJECT_NAME}...${E}")

if(NOT DEFINED IMAGINE_TARGET_LIB)
    set(IMAGINE_TARGET_LIB "Imagine_MapReduce")

    set(IS_MAKE_PROJECT false)
    set(IMAGINE_SYSTEM_DIR ${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/Imagine_System)
    set(IMAGINE_TOOL_DIR ${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/Imagine_Tool)
    set(IMAGINE_LIB_WORKER_DIR ${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/Imagine_System/worker)
    set(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/lib)

    if(EXISTS ${IMAGINE_TOOL_DIR}/CMakeLists.txt)
        add_subdirectory(${IMAGINE_TOOL_DIR})
        message("${B}[Constructing LIB][Imagine MapReduce]: thirdparty Imagine_Tool exists, add subModule...${E}")
    else()
        message("${R}[Constructing LIB][Imagine MapReduce]: thirdparty Imagine_Tool NOT exists, cin make init to init it!${E}")
    endif()

    if(EXISTS ${IMAGINE_SYSTEM_DIR}/CMakeLists.txt)
        add_subdirectory(${IMAGINE_SYSTEM_DIR})
        message("${B}[Constructing LIB][Imagine MapReduce]: thirdparty Imagine_system exists, add subModule...${E}")
    else()
        message("${R}[Constructing LIB][Imagine MapReduce]: thirdparty Imagine_System NOT exists, cin make init to init it!${E}")
    endif()
endif()

# 设置动态库的TARGET的头文件, 源文件, 依赖
file(GLOB IMAGINE_MAPREDUCE_SRC_LIST ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc)
add_library(imagine_mapreduce SHARED ${IMAGINE_MAPREDUCE_SRC_LIST})
target_include_directories(imagine_mapreduce PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(imagine_mapreduce imagine_rpc)

if(TARGET imagine_rpc)
    target_link_libraries(imagine_mapreduce imagine_rpc)
endif()