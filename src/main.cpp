// =====================================================================
//  Cadence demo driver.
//
//  Runs the Cadence sketch over a trace (or over a built-in synthetic
//  stream) and reports, for the sketch alone:
//
//    * detection quality against an exact SSI oracle
//      (recall / precision / F1, and the ARE of the rate estimate),
//    * the durable-SSI intervals the sketch emits,
//    * update throughput, on a separate untimed-bookkeeping pass.
//
//  The oracle here is evaluation code, not part of the sketch: it keeps a
//  ring of P+2 exact per-epoch count tables and applies the SSI
//  definition literally. The sketch itself is src/cadence.h.
//
//  Build:  make            Run:  ./cadence --help
// =====================================================================
#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <chrono>
#include <map>
#include <random>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "cadence.h"
#include "trace.h"

// Key width in bytes, fixed at compile time. 8 covers every trace shipped
// with the paper except the campus 5-tuple; build with -DCADENCE_KEYLEN=13
// for that one. Shorter keys are zero-padded to this width.
#ifndef CADENCE_KEYLEN
#define CADENCE_KEYLEN 8
#endif
static const int KEYLEN = CADENCE_KEYLEN;

typedef std::pair<std::string, int> Verdict;   // (key, epoch)

// ---------------------------------------------------------------------
// Exact SSI oracle (evaluation only).
//
// A verdict is recorded for (u, t) when u arrives in epoch t and the P
// completed epochs t-P .. t-1 satisfy all three conditions: every one of
// them active, arithmetic mean >= mu_min, and mean absolute deviation
// about that mean <= tau. This is the SSIL definition applied to the
// exact per-epoch counts, which is what the sketch approximates.
// ---------------------------------------------------------------------
class Oracle {
public:
    Oracle(int P, double muMin, double tau)
        : P_(P), muMin_(muMin), tau_(tau), ring_(P + 2), last_(0), tbl_(P + 2) {}

    void Insert(const std::string& id, int epoch) {
        const int slot = epoch % ring_;
        if (slot != last_) { tbl_[(epoch + 1) % ring_].clear(); last_ = slot; }

        const bool first = (tbl_[slot].find(id) == tbl_[slot].end());
        tbl_[slot][id]++;
        if (!first) return;

        std::vector<int> c(P_);
        long sum = 0;
        for (int i = 2; i < ring_; i++) {
            const std::unordered_map<std::string, int>& s = tbl_[(epoch + i) % ring_];
            std::unordered_map<std::string, int>::const_iterator it = s.find(id);
            const int v = (it == s.end()) ? 0 : it->second;
            if (v == 0) return;                       // not continuous
            c[i - 2] = v;
            sum += v;
        }
        const double mean = (double)sum / (double)P_;
        if (mean < muMin_) return;
        double mad = 0;
        for (int k = 0; k < P_; k++) mad += fabs((double)c[k] - mean);
        mad /= (double)P_;
        if (mad > tau_) return;

        const Verdict v(id, epoch);
        truth.insert(v);
        trueMean[v] = mean;
    }

    std::set<Verdict>        truth;
    std::map<Verdict, double> trueMean;

private:
    int    P_;
    double muMin_, tau_;
    int    ring_, last_;
    std::vector<std::unordered_map<std::string, int> > tbl_;
};

// ---------------------------------------------------------------------
// Built-in synthetic stream, so the repository runs without a dataset.
// `stable` items deliver a near-constant number of updates per epoch
// (these are the intended SSIs); the rest is Zipf-distributed noise over
// a large id space, plus a few high-rate but bursty items that satisfy
// continuity and significance while failing stability.
// ---------------------------------------------------------------------
static void Synthesize(std::vector<char>& keys, int epochs, int E,
                       int stable, int bursty, unsigned seed) {
    std::mt19937 rng(seed);
    std::uniform_real_distribution<double> uni(0.0, 1.0);
    keys.assign((size_t)epochs * (size_t)E * (size_t)KEYLEN, 0);

    std::vector<int> rate(stable);
    for (int i = 0; i < stable; i++) rate[i] = 9 + (int)(uni(rng) * 12);

    const int noiseIds = 200000;
    const double zipf = 1.1;
    std::vector<double> cdf(1000);
    double acc = 0;
    for (int i = 0; i < 1000; i++) { acc += 1.0 / pow(i + 1.0, zipf); cdf[i] = acc; }
    for (int i = 0; i < 1000; i++) cdf[i] /= acc;

    std::vector<unsigned> epochIds;
    size_t w = 0;
    for (int t = 0; t < epochs; t++) {
        epochIds.clear();
        for (int i = 0; i < stable; i++) {
            int n = rate[i] + (int)(uni(rng) * 3) - 1;       // jitter of +-1
            for (int k = 0; k < n; k++) epochIds.push_back(1u + (unsigned)i);
        }
        for (int i = 0; i < bursty; i++) {
            int n = (uni(rng) < 0.5) ? 2 : 60;               // same mean, high MAD
            for (int k = 0; k < n; k++)
                epochIds.push_back(1000000u + (unsigned)i);
        }
        while ((int)epochIds.size() < E) {
            const double r = uni(rng);
            int lo = 0, hi = 999;
            while (lo < hi) { int mid = (lo + hi) / 2; if (cdf[mid] < r) lo = mid + 1; else hi = mid; }
            epochIds.push_back(2000000u + (unsigned)(lo * 197 % noiseIds));
        }
        for (size_t i = epochIds.size(); i > 1; i--)          // shuffle within the epoch
            std::swap(epochIds[i - 1], epochIds[(size_t)(uni(rng) * (double)i)]);
        for (int k = 0; k < E; k++, w++)
            memcpy(&keys[w * (size_t)KEYLEN], &epochIds[k], 4);
    }
    keys.resize(w * (size_t)KEYLEN);
}

// Longest durable run first, for the digest printed at the end.
static bool LongerRun(const cadence::Interval& a, const cadence::Interval& b) {
    return (a.end - a.start) > (b.end - b.start);
}

// ---------------------------------------------------------------------
static void Usage() {
    printf("usage: cadence [options]\n"
           "  --trace FILE      binary trace; omit to use the synthetic stream\n"
           "  --format NAME     caida | caida_src | campus | campus_src | zipf | webdocs\n"
           "  --mem KB          memory budget for the table   (default 40)\n"
           "  --rows M          probes per update             (default 2)\n"
           "  --epoch E         updates per epoch             (default 10000)\n"
           "  --limit N         stop after N updates          (default all)\n"
           "  --seed S          sketch seed                   (default 1)\n"
           "  --P / --mumin / --tau / --alpha / --lambda / --L   task and sketch parameters\n"
           "  --passes N        timed throughput passes       (default 3)\n");
}

int main(int argc, char** argv) {
    std::string tracePath, format = "caida";
    double memKB = 40;
    int    rows = 2, E = 10000, passes = 3;
    long   limit = 0;
    unsigned seed = 1;
    cadence::Config cfg;

    for (int i = 1; i < argc; i++) {
        std::string a = argv[i];
        const char* v = (i + 1 < argc) ? argv[i + 1] : "";
        if      (a == "--trace")        { tracePath = v; i++; }
        else if (a == "--format")       { format = v; i++; }
        else if (a == "--mem")          { memKB = atof(v); i++; }
        else if (a == "--rows")         { rows = atoi(v); i++; }
        else if (a == "--epoch")        { E = atoi(v); i++; }
        else if (a == "--limit")        { limit = atol(v); i++; }
        else if (a == "--seed")         { seed = (unsigned)atoi(v); i++; }
        else if (a == "--P")            { cfg.P = atoi(v); i++; }
        else if (a == "--mumin")        { cfg.muMin = atof(v); i++; }
        else if (a == "--tau")          { cfg.tau = atof(v); i++; }
        else if (a == "--alpha")        { cfg.alpha = atof(v); i++; }
        else if (a == "--lambda")       { cfg.lambda = atof(v); i++; }
        else if (a == "--L")            { cfg.L = atoi(v); i++; }
        else if (a == "--passes")       { passes = atoi(v); i++; }
        else { Usage(); return (a == "--help" || a == "-h") ? 0 : 1; }
    }

    // ---- input ----------------------------------------------------
    std::vector<char> keys;
    size_t n = 0;
    if (!tracePath.empty()) {
        cadence::TraceFormat fmt;
        if (!cadence::TraceByName(format, fmt)) { printf("unknown format '%s'\n", format.c_str()); return 1; }
        if (fmt.keyBytes > KEYLEN)
            printf("warning: %s keys are %d B but this build stores %d; rebuild with "
                   "-DCADENCE_KEYLEN=%d\n", fmt.name, fmt.keyBytes, KEYLEN, fmt.keyBytes);
        const size_t cap = limit > 0 ? (size_t)limit : (size_t)-1;   // -1: whole file
        n = cadence::ReadTrace<KEYLEN>(tracePath, fmt, cap, keys);
        if (n == 0) return 1;
        printf("trace      %s  [%s]\n", tracePath.c_str(), fmt.name);
    } else {
        const int epochs = 200;
        Synthesize(keys, epochs, E, 120, 20, 7);
        n = keys.size() / (size_t)KEYLEN;
        if (limit > 0 && (size_t)limit < n) n = (size_t)limit;
        printf("trace      synthetic  [%d epochs x %d updates, 120 stable + 20 bursty items]\n",
               epochs, E);
    }

    // ---- sizing ---------------------------------------------------
    cadence::Cadence<KEYLEN> probe(1, 1, cfg, seed);
    int cols = (int)((size_t)(memKB * 1024.0)
                     / ((size_t)rows * probe.BucketBytes()));
    if (cols < 1) cols = 1;

    printf("updates    %zu  (%d per epoch, %d epochs)\n", n, E, (int)((n + E - 1) / E));
    printf("memory     %.1f KB -> %d rows x %d buckets\n",
           memKB, rows, cols);
    printf("parameters P=%d  mu_min=%g  tau=%g  alpha=%g  lambda=%g  L=%d  key=%d B\n\n",
           cfg.P, cfg.muMin, cfg.tau, cfg.alpha, cfg.lambda, cfg.L, KEYLEN);

    // ---- pass 1: accuracy -----------------------------------------
    cadence::Cadence<KEYLEN> sk(rows, cols, cfg, seed);
    Oracle oracle(cfg.P, cfg.muMin, cfg.tau);
    std::set<Verdict>         reported;
    std::map<Verdict, double> muEst;

    int epoch = 0;
    cadence::Report rep;
    for (size_t i = 0; i < n; i++) {
        const int t = (int)(i / (size_t)E);
        if (t != epoch) { epoch = t; sk.NewEpoch(t); }
        const char* k = &keys[i * (size_t)KEYLEN];
        const std::string id(k, KEYLEN);
        oracle.Insert(id, t);
        if (sk.Update(k, 1, &rep)) {
            const Verdict v(id, t);
            if (reported.insert(v).second) muEst[v] = rep.mu;
        }
    }
    sk.Flush();

    long hits = 0;
    double areSum = 0; long areN = 0;
    for (std::set<Verdict>::const_iterator it = reported.begin(); it != reported.end(); ++it) {
        if (!oracle.truth.count(*it)) continue;
        hits++;
        const double tm = oracle.trueMean[*it];
        if (tm > 1e-9) { areSum += fabs(muEst[*it] - tm) / tm; areN++; }
    }
    const double rr = oracle.truth.empty() ? 0 : (double)hits / (double)oracle.truth.size();
    const double pr = reported.empty()     ? 0 : (double)hits / (double)reported.size();
    const double f1 = (rr + pr > 0) ? 2 * rr * pr / (rr + pr) : 0;

    printf("ground truth   %zu SSI verdicts\n", oracle.truth.size());
    printf("reported       %zu verdicts, %ld correct\n", reported.size(), hits);
    printf("recall         %.4f\n", rr);
    printf("precision      %.4f\n", pr);
    printf("F1             %.4f\n", f1);
    printf("rate ARE       %.4f  (over %ld matched verdicts)\n\n", areN ? areSum / areN : 0.0, areN);

    // ---- durability ------------------------------------------------
    std::vector<cadence::Interval> runs = sk.DurableRuns();
    long longest = 0;
    for (size_t i = 0; i < runs.size(); i++) {
        const long len = runs[i].end - runs[i].start + 1;
        if (len > longest) longest = len;
    }
    printf("durable SSIs   %zu runs of length >= %d, longest %ld epochs\n",
           runs.size(), cfg.L, longest);
    std::sort(runs.begin(), runs.end(), LongerRun);
    for (size_t i = 0; i < runs.size() && i < 5; i++)
        printf("               %s  epochs [%d, %d]  (%d)\n",
               cadence::ToHex(runs[i].key).c_str(), runs[i].start, runs[i].end,
               runs[i].end - runs[i].start + 1);
    printf("\n");

    // ---- pass 2: throughput ---------------------------------------
    double best = 0;
    for (int p = 0; p < passes; p++) {
        cadence::Cadence<KEYLEN> timed(rows, cols, cfg, seed);
        const std::chrono::high_resolution_clock::time_point t0 =
            std::chrono::high_resolution_clock::now();
        int te = 0;
        for (size_t i = 0; i < n; i++) {
            const int t = (int)(i / (size_t)E);
            if (t != te) { te = t; timed.NewEpoch(t); }
            timed.Update(&keys[i * (size_t)KEYLEN]);
        }
        const double sec = std::chrono::duration<double>(
            std::chrono::high_resolution_clock::now() - t0).count();
        const double mops = (double)n / sec / 1e6;
        if (mops > best) best = mops;
    }
    printf("throughput     %.2f Mops  (best of %d passes, sweep included)\n", best, passes);
    return 0;
}
