#
# Copyright (C) 2026 Seastar contributors
#

# Locate the infiniband verbs library (libibverbs).
#
# Defines:
#   ibverbs_FOUND        - true if verbs.h and libibverbs were found
#   ibverbs_LIBRARIES    - the libraries to link against
#   ibverbs_INCLUDE_DIRS - where to find infiniband/verbs.h

find_path (ibverbs_INCLUDE_DIR
  NAMES infiniband/verbs.h)

find_library (ibverbs_LIBRARY
  NAMES ibverbs)

include (FindPackageHandleStandardArgs)
find_package_handle_standard_args (ibverbs
  REQUIRED_VARS
    ibverbs_LIBRARY
    ibverbs_INCLUDE_DIR)

mark_as_advanced (
  ibverbs_LIBRARY
  ibverbs_INCLUDE_DIR)

if (ibverbs_FOUND AND NOT (TARGET ibverbs::ibverbs))
  add_library (ibverbs::ibverbs UNKNOWN IMPORTED)
  set_target_properties (ibverbs::ibverbs
    PROPERTIES
      IMPORTED_LOCATION "${ibverbs_LIBRARY}"
      INTERFACE_INCLUDE_DDIRECTORIES "${ibverbs_INCLUDE_DIR}")
endif ()
