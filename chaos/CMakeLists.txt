add_library(chaos
        chaos.hpp
        chaos.cpp
        chaos_base.hpp
        )

target_link_libraries(chaos options)
add_dependencies(chaos openssl)
add_subdirectory(test)
