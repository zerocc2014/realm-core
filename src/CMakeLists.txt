add_subdirectory(realm)
add_subdirectory(external/IntelRDFPMathLib20U2)

add_library(cxxopts INTERFACE)
target_include_directories(cxxopts INTERFACE external/cxxopts/)

add_library(json INTERFACE)
target_include_directories(json INTERFACE external/json/)

if(CMAKE_SYSTEM_NAME MATCHES "^Windows")
  add_subdirectory(win32)
endif()

install(FILES realm.hpp
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
        COMPONENT devel)
