// =====================================================================
//  Minimal reader for the binary traces used in the paper.
//
//  Each trace file is a flat stream of fixed-size records; only the key
//  offset and width differ between datasets:
//
//    caida      16 B/record = [8 B timestamp][4 B srcIP][4 B dstIP]
//               flow key = (srcIP, dstIP), 8 B at offset 8
//    campus     13 B/record = 5-tuple, no timestamp, key at offset 0
//    zipf        4 B/record = bare item id
//    webdocs     8 B/record = item id, first 4 B used
//
//  Epochs are update-indexed: epoch = record_index / E, matching the
//  convention of the paper.
// =====================================================================
#ifndef CADENCE_TRACE_H
#define CADENCE_TRACE_H

#include <cstdio>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>

namespace cadence {

struct TraceFormat {
    int         recordBytes;
    int         keyOffset;
    int         keyBytes;
    const char* name;
};

inline bool TraceByName(const std::string& n, TraceFormat& out) {
    struct { const char* n; TraceFormat f; } table[] = {
        {"caida",      {16, 8, 8,  "caida (srcIP+dstIP)"}},
        {"caida_src",  {16, 8, 4,  "caida (srcIP)"}},
        {"campus",     {13, 0, 13, "campus (5-tuple)"}},
        {"campus_src", {13, 0, 4,  "campus (srcIP)"}},
        {"zipf",       {4,  0, 4,  "zipf"}},
        {"webdocs",    {8,  0, 4,  "webdocs"}},
    };
    for (size_t i = 0; i < sizeof(table) / sizeof(table[0]); i++)
        if (n == table[i].n) { out = table[i].f; return true; }
    return false;
}

// Read up to `cap` records into `keys`, each zero-padded to KEYLEN bytes.
// Returns the number of records read, or 0 if the file cannot be opened.
template <int KEYLEN>
size_t ReadTrace(const std::string& path, const TraceFormat& fmt, size_t cap,
                 std::vector<char>& keys) {
    std::ifstream f(path.c_str(), std::ios::binary | std::ios::ate);
    if (!f.is_open()) {
        fprintf(stderr, "cadence: cannot open trace '%s'\n", path.c_str());
        return 0;
    }
    const size_t avail = (size_t)(f.tellg() / fmt.recordBytes);
    f.seekg(0, std::ios::beg);
    if (avail < cap) cap = avail;

    const int kb = (fmt.keyBytes < KEYLEN) ? fmt.keyBytes : KEYLEN;
    keys.assign(cap * (size_t)KEYLEN, 0);
    char buf[64];
    size_t n = 0;
    while (n < cap && f.read(buf, fmt.recordBytes))
        memcpy(&keys[n++ * (size_t)KEYLEN], buf + fmt.keyOffset, (size_t)kb);
    keys.resize(n * (size_t)KEYLEN);
    return n;
}

}  // namespace cadence

#endif  // CADENCE_TRACE_H
