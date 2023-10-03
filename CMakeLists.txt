cmake_minimum_required(VERSION 3.0)
project(IMAGINE_MAPREDUCE)

if(TARGET init_all)
    message("Imagine System init already!")
else()
    set(IMAGINE_SYSTEM_DIR ${PROJECT_SOURCE_DIR}/thirdparty/Imagine_System)
    add_subdirectory(${IMAGINE_SYSTEM_DIR})
endif()

include_directories(${PROJECT_SOURCE_DIR}/include)
file(GLOB SRC_LIST ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)
set(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/lib)
add_library(imagine_mapreduce SHARED ${SRC_LIST})

if(TARGET init)
    
else()
    add_custom_target(
        init
        COMMAND python3 init.py
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR} # 在哪个目录下执行命令
    )
endif()

add_dependencies(imagine_mapreduce init_all)

target_link_libraries(imagine_mapreduce pthread)