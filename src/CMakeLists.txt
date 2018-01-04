include_directories (${LEVELDB_INCLUDE_DIR})
include_directories (include)
set (SRC
     example.cc
     multiple-store.cc)

add_executable (leveldb-allocator-example ${SRC})
target_link_libraries (leveldb-allocator-example ${LEVELDB_LIBRARIES})
