#include <iostream>
#include <ostream>
#include <vector>
#include <db/Query.hpp>

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

    ++it;
  }

}

void db::filter(const DbFile &in, DbFile &out, const std::vector<FilterPredicate> &pred) {
  // Save input and output file TD
  const TupleDesc &inputTd = in.getTupleDesc();
  const TupleDesc &outputTd = out.getTupleDesc();

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


void db::aggregate(const DbFile &in, DbFile &out, const Aggregate &agg) {
  // Save input and output file TD
  const TupleDesc &inputTd = in.getTupleDesc();
  const TupleDesc &outputTd = out.getTupleDesc();

  // Create vector to store all values from input tuple
  std::vector<field_t> aggVals;
  size_t fieldIndex = inputTd.index_of(agg.field);

  // Iterate through all tuples in the input file
  auto it = in.begin();
  while (it != in.end()) {
    const Tuple &inputTuple = *it;

    // Get value from current tuple into vector of values to aggregate
    field_t inputVal = inputTuple.get_field(fieldIndex);
    aggVals.push_back(inputVal);

    ++it;
  }

  // Check aggregator operator and perform across entire vector - TODO: Check for int or double
  AggregateOp op = agg.op;
  int resultVal = std::get<int>(aggVals[0]);
  double avgVal = 0.0;
  std::vector<field_t> outputTupleFields;
  field_t outputVal;
  int val = 0;
  switch (op) {
    case AggregateOp::MIN:
      // Loop through agg Vals
      for (auto & aggVal : aggVals){
        val = std::get<int>(aggVal);
        // Check if val < minVal
        if (val < resultVal) {
          resultVal = val;
        }
      }

      // Write single tuple to output file
      // Convert resultVal back to type field_t
      outputVal = resultVal;
      outputTupleFields.push_back(outputVal);
      out.insertTuple(Tuple(outputTupleFields));
      break;
    case AggregateOp::MAX:
      // Loop through agg Vals
      for (auto & aggVal : aggVals){
        val = std::get<int>(aggVal);
        // Check if val < minVal
        if (val > resultVal) {
          resultVal = val;
        }
      }

      // Write single tuple to output file
      // Convert resultVal back to type field_t
      outputVal = resultVal;
      outputTupleFields.push_back(outputVal);
      out.insertTuple(Tuple(outputTupleFields));
      break;
    case AggregateOp::AVG:
      // Iterate over aggVals, skip first value since it is already accounted for
      for (int i = 1; i < aggVals.size(); i++) {
        resultVal = resultVal + std::get<int>(aggVals[i]);
      }

      // Calculate average
      avgVal = resultVal / static_cast<double>(aggVals.size());
      outputVal = avgVal;

      // Output to file
      outputTupleFields.push_back(outputVal);
      out.insertTuple(Tuple(outputTupleFields));
      break;
    case AggregateOp::SUM:
      // Iterate over aggVals, skip first value since it is already accounted for
      for (int i = 1; i < aggVals.size(); i++) {
        resultVal = resultVal + std::get<int>(aggVals[i]);
      }

      // Sum is already calculated
      outputVal = resultVal;

      // Output to file
      outputTupleFields.push_back(outputVal);
      out.insertTuple(Tuple(outputTupleFields));
      break;
    case AggregateOp::COUNT:
      // Get size of aggregate value vector and write
      resultVal = aggVals.size();
      outputVal = resultVal;
      outputTupleFields.push_back(outputVal);
      out.insertTuple(Tuple(outputTupleFields));
      break;
    default:
      throw std::invalid_argument("Unknown aggregate op");
  }
}

void db::join(const DbFile &left, const DbFile &right, DbFile &out, const JoinPredicate &pred) {
  // TODO: Implement this function
}
