#include <db/Database.hpp>
#include <db/HeapFile.hpp>
#include <db/HeapPage.hpp>
#include <stdexcept>

using namespace db;

HeapFile::HeapFile(const std::string &name, const TupleDesc &td) : DbFile(name, td) {}

void HeapFile::insertTuple(const Tuple &t) {
    // Get last page of file
    Page page = Page{};
    readPage(page, numPages - 1);
    HeapPage hp = HeapPage(page, td);

    if (!hp.insertTuple(t)) {
        // If page is full, create a new page and insert tuple into it
        page = Page{};
        HeapPage newhp = HeapPage(page, td);
        newhp.insertTuple(t);
        numPages++;
    }

    // Write page to disk, new or existing
    writePage(page, numPages - 1);
}

void HeapFile::deleteTuple(const Iterator &it) {
    // TODO pa1
}

Tuple HeapFile::getTuple(const Iterator &it) const {
    // TODO pa1
}

void HeapFile::next(Iterator &it) const {
    // TODO pa1
}

Iterator HeapFile::begin() const {
    // Return iterator to first populated tuple in file. Iterator should consist of a page number and slot number

    // iterate until numPages
    for (int i = 0; i < numPages; ++i) {
        // Read ith page from this file
        Page page = Page{};
        readPage(page, i);

        // Wrap page as heap page
        HeapPage hp = HeapPage(page, td);

        // Check for first populated tuple in current page
        size_t slot = hp.begin();
        if (slot != hp.end()) {
            return Iterator(*this, i, slot);
        }
    }

    // If no tuples found, return end iterator
    return end();
}

Iterator HeapFile::end() const {
    // Read first page from file
    Page p = Page{};
    readPage(p, 0);

    HeapPage hp = HeapPage(p, td);

    // Return iterator to final page and final slot
    return Iterator(*this, numPages - 1, hp.end());
}
