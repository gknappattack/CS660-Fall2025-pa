#include <iostream>
#include <ostream>
#include <vector>
#include <db/Query.hpp>
#include <map>

using namespace db;

void db::projection(const DbFile &in, DbFile &out, const std::vector<std::string> &field_names) {
  // Save input and output file TD
  const TupleDesc &inputTd = in.getTupleDesc();
  const TupleDesc &outputTd = out.getTupleDesc();

  // Create a mapping from output field positions to input field positions
  std::vector<size_t> fieldMapping;
  fieldMapping.reserve(field_names.size());
  for (const auto &fieldName : field_names) {
    fieldMapping.push_back(inputTd.index_of(fieldName));
  }

  // Iterate through all tuples in the input file
  auto it = in.begin();
  while (it != in.end()) {
    const Tuple &inputTuple = *it;

    // Create a new tuple for the output with only the requested fields
    std::vector<field_t> outputTupleFields;
    outputTupleFields.reserve(fieldMapping.size());

    for (size_t i : fieldMapping) {
      outputTupleFields.push_back(inputTuple.get_field(i));
    }

    // Insert the projected tuple into the output file
    out.insertTuple(Tuple(outputTupleFields));

    // Increment input tuple
    ++it;
  }

}

void db::filter(const DbFile &in, DbFile &out, const std::vector<FilterPredicate> &pred) {
  // Save input and output file TD
  const TupleDesc &inputTd = in.getTupleDesc();

  // Iterate through all tuples in the input file
  auto it = in.begin();
  while (it != in.end()) {
    const Tuple &inputTuple = *it;

    // Bool to track if any predicates are failed
    bool passesCondtions = true;
    // Loop through all filter predicates
    for (const auto& currentPred : pred) {
      // Check if current tuple fulfills given predicate condition
      PredicateOp op = currentPred.op;
      size_t fieldIndex = inputTd.index_of(currentPred.field_name);

      field_t valueToCompare = inputTuple.get_field(fieldIndex);
      field_t predVal = currentPred.value;

      switch (op) {
        case PredicateOp::EQ:
          passesCondtions = valueToCompare == predVal;
          break;
        case PredicateOp::NE:
          passesCondtions = valueToCompare != predVal;
          break;
        case PredicateOp::LT:
          passesCondtions = valueToCompare < predVal;
          break;
        case PredicateOp::LE:
          passesCondtions = valueToCompare <= predVal;
          break;
        case PredicateOp::GT:
          passesCondtions = valueToCompare > predVal;
          break;
        case PredicateOp::GE:
          passesCondtions = valueToCompare >= predVal;
          break;
        default:
          throw std::invalid_argument("Unknown predicate op");
        }

      // If any conditions fail, stop checking and break
      if (!passesCondtions) {
        break;
      }
    }

    // Insert the projected tuple into the output file if all conditions passed
    if (passesCondtions) {
      out.insertTuple(inputTuple);
    }
    ++it;
  }
}

// Helper function to compute aggregation on a vector of values
field_t computeAggregation(AggregateOp op, const std::vector<field_t>& aggVals) {
  if (aggVals.empty()) {
    throw std::invalid_argument("Cannot aggregate empty group");
  }

  int resultVal = std::get<int>(aggVals[0]);

  switch (op) {
    case AggregateOp::MIN:
      for (const auto& aggVal : aggVals) {
        int val = std::get<int>(aggVal);
        if (val < resultVal) {
          resultVal = val;
        }
      }
      return resultVal;

    case AggregateOp::MAX:
      for (const auto& aggVal : aggVals) {
        int val = std::get<int>(aggVal);
        if (val > resultVal) {
          resultVal = val;
        }
      }
      return resultVal;

    case AggregateOp::AVG: {
      for (size_t i = 1; i < aggVals.size(); i++) {
        resultVal += std::get<int>(aggVals[i]);
      }
      double avgVal = resultVal / static_cast<double>(aggVals.size());
      return avgVal;
    }

    case AggregateOp::SUM:
      for (size_t i = 1; i < aggVals.size(); i++) {
        resultVal += std::get<int>(aggVals[i]);
      }
      return resultVal;

    case AggregateOp::COUNT:
      return static_cast<int>(aggVals.size());

    default:
      throw std::invalid_argument("Unknown aggregate op");
  }
}

// Helper function for non-grouped aggregation
void performAggregation(AggregateOp op, const std::vector<field_t>& aggVals, DbFile& out) {
  field_t result = computeAggregation(op, aggVals);
  std::vector<field_t> outputTupleFields;
  outputTupleFields.push_back(result);
  out.insertTuple(Tuple(outputTupleFields));
}

void db::aggregate(const DbFile &in, DbFile &out, const Aggregate &agg) {
  // Save input and output file TD
  const TupleDesc &inputTd = in.getTupleDesc();

  // Get the index of the field to aggregate on
  size_t fieldIndex = inputTd.index_of(agg.field);

  // Check if we're grouping
  if (agg.group == std::nullopt) {
    // No grouping aggregate all values into one result
    std::vector<field_t> aggVals;

    // Iterate through all tuples in the input file
    auto it = in.begin();
    while (it != in.end()) {
      const Tuple &inputTuple = *it;
      field_t inputVal = inputTuple.get_field(fieldIndex);
      aggVals.push_back(inputVal);
      ++it;
    }

    // Perform aggregation and output single tuple
    performAggregation(agg.op, aggVals, out);

  } else {
    // We are grouping - create a map of group values to vectors of aggregate values
    size_t groupIndex = inputTd.index_of(agg.group.value());
    std::map<field_t, std::vector<field_t>> groupMap;

    // Iterate through all tuples and organize by group
    auto it = in.begin();
    while (it != in.end()) {
      const Tuple &inputTuple = *it;

      field_t groupValue = inputTuple.get_field(groupIndex);
      field_t aggValue = inputTuple.get_field(fieldIndex);

      // Add to the appropriate group
      groupMap[groupValue].push_back(aggValue);

      ++it;
    }

    // Now process each group and output one tuple per group
    for (const auto &[groupValue, aggVals] : groupMap) {
      std::vector<field_t> outputTupleFields;

      // Push back the group value
      outputTupleFields.push_back(groupValue);

      // Push back the aggregated value for the group
      field_t aggResult = computeAggregation(agg.op, aggVals);
      outputTupleFields.push_back(aggResult);

      // Write tuple to the output file
      out.insertTuple(Tuple(outputTupleFields));
    }
  }
}

void db::join(const DbFile &left, const DbFile &right, DbFile &out, const JoinPredicate &pred) {
  // Get TupleDesc objects for the left and right files.
  const TupleDesc &leftTd = left.getTupleDesc();
  const TupleDesc &rightTd = right.getTupleDesc();
  const TupleDesc &outTd = out.getTupleDesc();

  // Get indices of left and right join column
  size_t leftJoinCol = leftTd.index_of(pred.left);
  size_t rightJoinCol = rightTd.index_of(pred.right);

  bool isEQ = pred.op == PredicateOp::EQ;

  // Iterate over all tuples in left file
  auto left_it = left.begin();
  while (left_it != left.end()) {
    const Tuple &leftTuple = *left_it;

    // Iterate over all tuples in right file - Check each right
    auto right_it = right.begin();
    while (right_it != right.end()) {
      const Tuple &rightTuple = *right_it;

      bool shouldJoin = false;

      // Check if tuple should be joined based on pred condition
      switch (pred.op) {
        case PredicateOp::EQ:
          if (leftTuple.get_field(leftJoinCol) == rightTuple.get_field(rightJoinCol)) {
            shouldJoin = true;
          }
          break;
        case PredicateOp::NE:
          if (leftTuple.get_field(leftJoinCol) != rightTuple.get_field(rightJoinCol)) {
            shouldJoin = true;
          }
          break;
        case PredicateOp::GT:
          if (leftTuple.get_field(leftJoinCol) > rightTuple.get_field(rightJoinCol)) {
            shouldJoin = true;
          }
          break;
        case PredicateOp::LT:
          if (leftTuple.get_field(leftJoinCol) < rightTuple.get_field(rightJoinCol)) {
            shouldJoin = true;
          }
          break;
        case PredicateOp::LE:
          if (leftTuple.get_field(leftJoinCol) <= rightTuple.get_field(rightJoinCol)) {
            shouldJoin = true;
          }
          break;
        case PredicateOp::GE:
          if (leftTuple.get_field(leftJoinCol) >= rightTuple.get_field(rightJoinCol)) {
            shouldJoin = true;
          }
          break;
        default:
          throw std::invalid_argument("Unknown predicate op");
      }

      // If tuple is a match
      if (shouldJoin) {
        // Vector of fields to create output tuple
        std::vector<field_t> joinedTupleFields;
        joinedTupleFields.reserve(outTd.size());

        // Write left tuple values into joinedTupleFields
        for (size_t i = 0; i < leftTd.size(); i++) {
          // Check if current field is in outTd schema
          joinedTupleFields.push_back(leftTuple.get_field(i));
        }

        // Write right tuple values into joinedTupleFields, if isEQ == true, then skip the filed at rightJoinCol
        for (size_t i = 0; i < rightTd.size(); i++) {
          if (isEQ == true) {
            if (i == rightJoinCol) {
              continue;
            }
          }
          joinedTupleFields.push_back(rightTuple.get_field(i));
        }

        // Write output tuple to file
        out.insertTuple(Tuple(joinedTupleFields));
      }

      // Increment right iterator
      ++right_it;
    }
    // Increment left iterator after all right tuples are checked
    ++left_it;
  }
}