//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// property_enforcer.cpp
//
// Identification: src/optimizer/property_enforcer.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/property_enforcer.h"
#include "optimizer/operators.h"
#include "optimizer/property.h"
#include "optimizer/properties.h"
#include "expression/expression_util.h"

namespace peloton {
namespace optimizer {

std::shared_ptr<GroupExpression> PropertyEnforcer::EnforceProperty(
    std::shared_ptr<GroupExpression> gexpr, PropertySet *properties,
    std::shared_ptr<Property> property) {
  input_gexpr_ = gexpr;
  input_properties_ = properties;
  property->Accept(this);
  return output_gexpr_;
}

void PropertyEnforcer::Visit(const PropertyColumns *) {
  std::vector<GroupID> child_groups(1, input_gexpr_->GetGroupID());

  output_gexpr_ =
      std::make_shared<GroupExpression>(PhysicalProject::make(), child_groups);
}

void PropertyEnforcer::Visit(const PropertyProjection *) {
  //  std::vector<GroupID> child_groups(1, input_gexpr_->GetGroupID());
  //
  //  output_gexpr_ =
  //      std::make_shared<GroupExpression>(PhysicalProject::make(),
  //      child_groups);
}

void PropertyEnforcer::Visit(const PropertySort *) {
  std::vector<GroupID> child_groups(1, input_gexpr_->GetGroupID());
  output_gexpr_ =
      std::make_shared<GroupExpression>(PhysicalOrderBy::make(), child_groups);
}

// TODO add implementation for PropertyDistinct and PropertyPredicate
void PropertyEnforcer::Visit(const PropertyDistinct *) {
  std::vector<GroupID> child_groups(1, input_gexpr_->GetGroupID());
  output_gexpr_ =
      std::make_shared<GroupExpression>(PhysicalDistinct::make(), child_groups);
}

void PropertyEnforcer::Visit(const PropertyPredicate *) {}

} /* namespace optimizer */
} /* namespace peloton */
