# Cadence -- reference implementation.
#
#   make                 build ./cadence with 8-byte keys
#   make KEYLEN=4        build for 4-byte keys (zipf, webdocs, campus_src)
#   make KEYLEN=13       build for the campus 5-tuple
#   make run             build and run the built-in synthetic stream
#   make clean
#
# Changing KEYLEN triggers a rebuild on its own; no need to clean first.
#
# The sketch itself is header-only: to use it in your own code just add
# src/ to the include path and #include "cadence.h".

CXX      ?= g++
CXXFLAGS ?= -O3 -std=c++11 -Wall
KEYLEN   ?= 8

HEADERS = src/cadence.h src/murmurhash.h src/trace.h

all: cadence

cadence: src/main.cpp $(HEADERS) .keylen
	$(CXX) $(CXXFLAGS) -DCADENCE_KEYLEN=$(KEYLEN) src/main.cpp -o $@

# Records the key width the current binary was built with, so that a change
# of KEYLEN invalidates it.
.keylen: FORCE
	@echo "$(KEYLEN)" | cmp -s - $@ 2>/dev/null || echo "$(KEYLEN)" > $@

run: cadence
	./cadence

clean:
	rm -f cadence .keylen

FORCE:

.PHONY: all run clean FORCE
