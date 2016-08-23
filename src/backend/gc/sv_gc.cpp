//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sv_gc.h
//
// Identification: src/backend/gc/sv_gc.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/gc/sv_gc.h"

namespace peloton {
namespace gc {

thread_local SV_GCContext current_gc_context;

}
}