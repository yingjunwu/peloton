
//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bwtree.cpp
//
// Identification: src/index/bwtree.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "bwtree.h"

#ifdef BWTREE_PELOTON
namespace peloton {
namespace index {
#endif

// This flag is only defined when bwtree debug flag is enabled
#ifdef BWTREE_DEBUG
bool print_flag = true;
#endif

#ifdef BWTREE_PELOTON
}  // End index namespace
}  // End peloton namespace
#endif
