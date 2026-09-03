    cmake_minimum_required(VERSION 3.10)
    project(WindFlowTableAPI_Test VERSION 1.0)

    set(CMAKE_CXX_STANDARD 17)
    set(CMAKE_CXX_STANDARD_REQUIRED True)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -Wall")

    find_package(Threads REQUIRED)

    include_directories(${CMAKE_SOURCE_DIR}/fastflow)

    include_directories(${CMAKE_SOURCE_DIR}/WindFlow/wf)
    include_directories(${CMAKE_SOURCE_DIR}/builder_pof)
    
    #operazioni
    add_executable(kg_group operazioni/kg_group.cpp)
    add_executable(g_group operazioni/g_group.cpp)
    add_executable(w_group operazioni/w_group.cpp)
    add_executable(w_join operazioni/w_join.cpp)
    add_executable(int_join operazioni/int_join.cpp)
    add_executable(distinct operazioni/distinct.cpp)
    add_executable(union_all operazioni/union_all.cpp)
    add_executable(union operazioni/union.cpp)
    add_executable(intersect operazioni/intersect.cpp)
    #[=[ add_executable(order operazioni/order.cpp) ]=]

    #builders
    add_executable(basic_pof builder_pof/basic_pof.cpp)
    add_executable(group_pof builder_pof/group_pof.cpp)
    add_executable(distinct_pof builder_pof/distinct_pof.cpp)
    add_executable(join_pof builder_pof/join_pof.cpp)
    add_executable(intersect_pof builder_pof/intersect_pof.cpp)
    add_executable(state_cleaning_pof builder_pof/state_cleaning_pof.cpp)
    add_executable(source_pof builder_pof/source_pof.cpp)
    
    target_link_libraries(kg_group Threads::Threads)
    target_link_libraries(g_group Threads::Threads)
    target_link_libraries(w_group Threads::Threads)
    target_link_libraries(w_join Threads::Threads)
    target_link_libraries(int_join Threads::Threads)
    target_link_libraries(distinct Threads::Threads)
    target_link_libraries(union_all Threads::Threads)
    target_link_libraries(union Threads::Threads)
    target_link_libraries(intersect Threads::Threads)
    #[=[ target_link_libraries(order Threads::Threads) ]=]

    target_link_libraries(basic_pof Threads::Threads)
    target_link_libraries(group_pof Threads::Threads)
    target_link_libraries(distinct_pof Threads::Threads)
    target_link_libraries(join_pof Threads::Threads)
    target_link_libraries(intersect_pof Threads::Threads)
    target_link_libraries(state_cleaning_pof Threads::Threads)
    target_link_libraries(source_pof Threads::Threads)
    
    