find_path(LEVELDB_INCLUDE_DIR NAMES leveldb/db.h PATHS "$ENV{LEVELDB_DIR}/include")
find_library(LEVELDB_LIBRARIES NAMES leveldb PATHS "$ENV{LEVELDB_DIR}/lib")

INCLUDE(FindPackageHandleStandardArgs)
find_package_handle_standard_args(LEVELDB DEFAULT_MSG LEVELDB_INCLUDE_DIR LEVELDB_LIBRARIES)

IF(LEVELDB_FOUND)
    MESSAGE(STATUS "Found LevelDB at ${LEVELDB_INCLUDE_DIR}")
    MARK_AS_ADVANCED(LEVELDB_INCLUDE_DIR LEVELDB_LIBRARIES)
ENDIF()