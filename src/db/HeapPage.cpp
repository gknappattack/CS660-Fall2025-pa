#include <db/Database.hpp>
#include <db/HeapPage.hpp>
#include <stdexcept>

using namespace db;

HeapPage::HeapPage(Page &page, const TupleDesc &td) : td(td) {
    // NOTE: header and data should point to locations inside the page buffer. Do not allocate extra memory.

    // Capacity is the number of tuples that fit in the page, calculated: P * 8 / (T * 8 + 1)
    capacity = (page.size() * 8) / (this->td.length() * 8 + 1);

    //The header points to the beginning of the page (zero offset in the page buffer)
    header = page.data();

    // Data is located at P - (C * T) where P is the size of a page, C is the number of tuples, and T is the size of a tuple
    size_t offset = page.size() - (capacity * this->td.length());
    data = header + offset;
}

size_t HeapPage::begin() const {
    // The begin method returns an iterator to the first populated tuple in the file. This might not be in the first page of the file.
    // Starting from data, check td.length until a valid tuple is found
    size_t slot = 0;
    while (slot < capacity && empty(slot)) {
        slot++;
    }
    return slot;
}

size_t HeapPage::end() const {
    // returns the index to the end of the page (capacity)
    return capacity;
}

bool HeapPage::insertTuple(const Tuple &t) {
    // Get first available slot in tuple
    size_t slot = 0;
    while (!empty(slot) && slot < capacity) {
        slot++;
    }

    // If slot == capacity, return false - tuple cannot be inserted
    if (slot == capacity) {
        return false;
    }

    // Serialize tuple into current slot
    this->td.serialize(data + slot * td.length(), t);

    // Set bit in header to 1 now that tuple is set
    header[slot / 8] |= (1 << (7 - (slot % 8)));

    return true;
}

void HeapPage::deleteTuple(size_t slot) {
    // Check if tuple at this slot is not empty
    if (empty(slot)) {
        throw std::logic_error("cannot delete non-existent tuple");
    }

    // Set bit in header to 0 now that tuple is deleted
    header[slot / 8] &= ~(1 << (7 - (slot % 8)));
}

Tuple HeapPage::getTuple(size_t slot) const {
    // Get appropriate offset pointer for current slot
    size_t offset = slot * td.length();

    // Call deserialize at current offset
    return td.deserialize(data + offset);
}

void HeapPage::next(size_t &slot) const {
    // Advance the slot by one
    slot++;

    // Iterate again until a populated tuple is found or capacity is reached
    while (empty(slot) && slot < capacity) {
        slot++;
    }
}

bool HeapPage::empty(size_t slot) const {
    // Check if slot % 8 th bit in header[slot % 8] is set to 0
    size_t offset = 7 - (slot % 8);
    return (header[slot / 8] & (1 << (offset))) == 0;
}
