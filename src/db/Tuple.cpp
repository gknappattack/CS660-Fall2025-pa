#include <algorithm>
#include <cstring>
#include <db/Tuple.hpp>
#include <stdexcept>
#include <vector>

using namespace db;

Tuple::Tuple(const std::vector<field_t> &fields) : fields(fields) {}

type_t Tuple::field_type(size_t i) const {
    const field_t &field = fields.at(i);
    if (std::holds_alternative<int>(field)) {
        return type_t::INT;
    }
    if (std::holds_alternative<double>(field)) {
        return type_t::DOUBLE;
    }
    if (std::holds_alternative<std::string>(field)) {
        return type_t::CHAR;
    }
    throw std::logic_error("Unknown field type");
}

size_t Tuple::size() const { return fields.size(); }

const field_t &Tuple::get_field(size_t i) const { return fields.at(i); }

TupleDesc::TupleDesc(const std::vector<type_t> &types, const std::vector<std::string> &names) {
    // Validate if a valid schema

    // 1. Names must be unique
    for (int i = 0; i < names.size(); ++i) {
        for (int j = i + 1; j < names.size(); ++j) {
            if (names[i] == names[j]) {
                throw std::invalid_argument("Tuple schema names must be unique");
            }
        }
    }

    // 2. Types and names must be the same length
    if (types.size() != names.size()) {
        throw std::invalid_argument("Tuple schema types and names must be the same length");
    }

    // If valid schema, build map and vectors
    for (int i = 0; i < types.size(); ++i) {
        tupleSchema.insert({names[i], i});
        tupleTypes.push_back(types.at(i));
        tupleNames.push_back(names.at(i));
    }
}

bool TupleDesc::compatible(const Tuple &tuple) const {
    // Check if tuple field_size.size() matches tupleTypes.size(), don't even iterate over mismatched lengths.
    if (tuple.size() != tupleTypes.size()) {
        return false;
    }

    // Iterate over tuple vector of fields
    for (int i = 0; i < tuple.size(); ++i) {
        // Get field type
        type_t type = tuple.field_type(i);

        // Get expected type at this index
        type_t expectedType = tupleTypes.at(i);

        // If types are not equal, return false
        if (type != expectedType) {
            return false;
        }
    }
    return true;
}

size_t TupleDesc::index_of(const std::string &name) const {
    // Search for name in names vector
    for (int i = 0; i < tupleNames.size(); ++i) {
        if (tupleNames.at(i) == name) {
            return i;
        }
    }
    throw std::out_of_range("Name not found");
}

size_t TupleDesc::offset_of(const size_t &index) const {
    // If index is 0, return 0 since it is first value in tuple
    if (index == 0) {
        return 0;
    }

    // If index > size - 1 of tupleTypes, throw exception
    if (index > tupleTypes.size() - 1) {
        throw std::out_of_range("Index out of range");
    }

    size_t offset = 0;

    for (int i = 0; i < index; ++i) {

        // Check enum value of
        type_t type = tupleTypes.at(i);

        // Add offset based on type
        if (type == type_t::INT) {
            offset += INT_SIZE;
        } else if (type == type_t::DOUBLE) {
            offset += DOUBLE_SIZE;
        } else if (type == type_t::CHAR) {
            offset += CHAR_SIZE;
        } else {
            throw std::logic_error("Unknown field type");
        }
    }
    return offset;
}

size_t TupleDesc::length() const {
    // Iterate over tuple types and sum lengths
    size_t len = 0;

    for (auto type : tupleTypes) {
        if (type == type_t::CHAR) {
            len += CHAR_SIZE;
        } else if (type == type_t::INT) {
            len += INT_SIZE;
        } else if (type == type_t::DOUBLE) {
            len += DOUBLE_SIZE;
        } else {
            throw std::logic_error("Unknown field type");
        }
    }

    return len;
}

size_t TupleDesc::size() const {
    return tupleTypes.size();
}

Tuple TupleDesc::deserialize(const uint8_t *data) const {
    // Iterate over tuple types, read values from data byte array into the Tuple object
    std::vector<field_t> fields;

    for (auto type : tupleTypes) {
        // Depending on type, read type_t size bytes from *data array
        if (type == type_t::CHAR) {
            char *str = new char[CHAR_SIZE];
            memcpy(str, data, CHAR_SIZE);
            fields.emplace_back(str);
            data += CHAR_SIZE;

            free(str);
        } else if (type == type_t::INT) {
            int* num = new int;
            memcpy(num, data, INT_SIZE);
            fields.emplace_back(*num);
            data += INT_SIZE;

            free(num);
        } else if (type == type_t::DOUBLE) {
            double *num = new double;
            memcpy(num, data, DOUBLE_SIZE);
            fields.emplace_back(*num);
            data += DOUBLE_SIZE;

            free(num);
        } else {
            throw std::logic_error("Unknown field type");
        }
    }

    return Tuple(fields);
}

void TupleDesc::serialize(uint8_t *data, const Tuple &t) const {
    //
    size_t offset = 0;
    int i = 0;

    // Iterate over field types in tuple desc
    for (auto type : tupleTypes) {
        if (type == type_t::CHAR) {
            // Get string from tuple at i
            std::string str = std::get<std::string>(t.get_field(i));

            // Write string to data at offset
            memcpy(data + offset, str.c_str(), CHAR_SIZE);
            offset += CHAR_SIZE;
            i++;
        } else if (type == type_t::DOUBLE) {
            // Get double from tuple at i
            double num = std::get<double>(t.get_field(i));

            // Write double to data at offset
            memcpy(data + offset, &num, DOUBLE_SIZE);
            offset += DOUBLE_SIZE;
            i++;
        } else if (type == type_t::INT) {
            // Get int from tuple at i
            int num = std::get<int>(t.get_field(i));

            // Write int to data at offset
            memcpy(data + offset, &num, INT_SIZE);
            offset += INT_SIZE;
            i++;
        } else {
            throw std::logic_error("Unknown field type"); // This should never occur at this point but in case
        }
    }
}

db::TupleDesc TupleDesc::merge(const TupleDesc &td1, const TupleDesc &td2) {
    // Check if there are duplicate names in either tuple
    for (int i = 0; i < td1.size(); ++i) {
        for (int j = 0; j < td2.size(); ++j) {
            if (td1.tupleNames.at(i) == td2.tupleNames.at(j)) {
                throw std::invalid_argument("Tuple schema names must be unique");
            }
        }
    }

    // If there are no duplicate names then create a new TD object using td1's names and types then td2's in order
    std::vector<type_t> types;
    std::vector<std::string> names;

    for (int i = 0; i < td1.size(); ++i) {
        types.push_back(td1.tupleTypes.at(i));
        names.push_back(td1.tupleNames.at(i));
    }

    for (int i = 0; i < td2.size(); ++i) {
        types.push_back(td2.tupleTypes.at(i));
        names.push_back(td2.tupleNames.at(i));
    }

    return TupleDesc(types, names);
}
