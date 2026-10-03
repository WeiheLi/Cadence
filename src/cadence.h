// =====================================================================
//  Cadence: Real-Time Sustained Stable Item Lookup in High-Speed Data Streams
//
//  Header-only reference implementation of the Cadence sketch. This file
//  contains the mechanism described in the paper and nothing else: no
//  baselines, no comparison harness, no evaluation instrumentation.
//
//  ---------------------------------------------------------------------
//  The task (SSIL). Time is cut into fixed epochs of E updates. Let x_t(u)
//  be the quantity item u delivers in epoch t (its update count, or the
//  total weight for the weighted variant). Item u is a Sustained Stable
//  Item (SSI) at epoch t when all three hold:
//
//      continuity     x_{t-i}(u) > 0     for i = 0..P-1
//      significance   mu_t(u)   >= mu_min
//      stability      MAD_t(u)  <= tau
//
//  where mu_t and MAD_t are exponentially weighted estimates of the mean
//  and the mean absolute deviation of x over the recent epochs.
//
//  ---------------------------------------------------------------------
//  The sketch. One table of m rows x d buckets. Each bucket holds one
//  candidate item and the three descriptors that decide the three gates:
//  a continuity streak S, an EWMA mean mu, and an EWMA mean absolute
//  deviation mad. No per-item history is stored and no second table is
//  needed, so an update is O(1) and the space is Theta(m*d).
//
//      Update  (Algorithm 1)  probe m buckets; on a match accumulate into
//                             the in-progress epoch counter; on an empty
//                             bucket insert; otherwise pick a victim by
//                             the retention score
//                                 score = S/P + mu/mu_min - mad/tau
//                             and replace it with probability 2^(-lambda *
//                             score), never replacing a bucket that
//                             currently satisfies all three gates.
//
//      Query   (Algorithm 2)  sweep the table once per epoch, committing
//                             each bucket's completed epoch, and report
//                             every resident that satisfies the gates.
//
//  The per-epoch sweep is mandatory, not an optimisation: a bucket that
//  receives no update during an epoch is brought up to date only by the
//  sweep, and the continuity streak would otherwise be wrong. Call
//  NewEpoch(t) (or Query(t), which subsumes it) exactly once per epoch.
//
//  ---------------------------------------------------------------------
//  Two extensions, both in this file:
//
//      Cadence-W    weighted streams. Pass a weight to Update(); the
//                   accumulator adds w instead of 1 and a fresh bucket is
//                   initialised at w. Nothing else changes, so x_t(u)
//                   becomes the total weight u delivers in epoch t.
//
//      Durability   how long an item stays an SSI. Two extra words per
//                   bucket (a qualified-epoch streak Q and a start tag
//                   ts) yield the maximal intervals over which an item
//                   qualifies, with no auxiliary structure.
//
//  ---------------------------------------------------------------------
//  Usage:
//
//      cadence::Config cfg;                        // paper defaults
//      cadence::Cadence<8> sk(2, d, cfg);          // 2 rows, 8-byte keys
//
//      for (each update) {
//          if (epoch changed) sk.NewEpoch(epoch);  // mandatory sweep
//          sk.Update(key);                         // or Update(key, w)
//      }
//      std::vector<cadence::Report> ssi;
//      sk.Query(epoch, ssi);                       // current SSIs
//      sk.Flush();                                 // close open runs
//      const std::vector<cadence::Interval>& runs = sk.DurableRuns();
// =====================================================================
#ifndef CADENCE_H
#define CADENCE_H

#include <cmath>
#include <cstdint>
#include <cstring>
#include <map>
#include <random>
#include <string>
#include <vector>

#include "murmurhash.h"

namespace cadence {

// ---------------------------------------------------------------------
// Parameters. The defaults are the ones used throughout the paper.
// ---------------------------------------------------------------------
struct Config {
    int      P        = 12;             // continuity: epochs of consecutive activity
    double   muMin    = 8.0;            // significance threshold
    double   tau      = 2.2360679775;   // stability threshold (sqrt(5))
    double   alpha    = 0.1;            // EWMA smoothing factor
    double   lambda   = 16.0;           // sharpness of the replacement decay
    int      L        = 2;              // durability: shortest run reported
    unsigned hashSeed = 0x100;          // base seed of the row hash family
};

// One reported SSI, with the descriptors the sketch holds for it.
struct Report {
    std::string key;         // raw key bytes
    int         S   = 0;     // continuity streak
    double      mu  = 0;     // EWMA mean of the per-epoch quantity
    double      mad = 0;     // EWMA mean absolute deviation
};

// One maximal run of consecutive qualifying epochs, inclusive at both ends.
struct Interval {
    std::string key;
    int         start = -1;
    int         end   = -1;
};

// Convenience: raw key bytes as lowercase hex.
inline std::string ToHex(const std::string& raw) {
    static const char* H = "0123456789abcdef";
    std::string s(raw.size() * 2, '0');
    for (size_t i = 0; i < raw.size(); i++) {
        unsigned char b = (unsigned char)raw[i];
        s[2 * i] = H[b >> 4];
        s[2 * i + 1] = H[b & 15];
    }
    return s;
}

// ---------------------------------------------------------------------
// One bucket. Fields are ordered widest-first and the two small counters
// are shorts, so the only padding is the tail alignment. Measured sizes
// (the demo prints sizeof at startup):
//
//   KEYLEN = 4   ->  32 B      KEYLEN = 8   ->  36 B
//   KEYLEN = 13  ->  44 B      KEYLEN = 15  ->  44 B
// ---------------------------------------------------------------------
template <int KEYLEN>
struct Bucket {
    char  key[KEYLEN];
    int   T;      // epoch tag of the in-progress accumulator
    int   C;      // in-progress quantity for epoch T (count, or weight)
    float mu;     // EWMA mean
    float mad;    // EWMA mean absolute deviation
    int   ts;     // durability: first epoch of the current qualifying run
    short S;      // continuity streak (saturates; only S >= P is ever tested)
    short Q;      // durability: length of the current qualifying run
    bool  occ;    // bucket holds an item
};

// ---------------------------------------------------------------------
// The sketch. KEYLEN is the key width in bytes: keys are compared and
// hashed over exactly this many bytes, so pad shorter keys with zeros.
// ---------------------------------------------------------------------
template <int KEYLEN = 15>
class Cadence {
public:
    typedef Bucket<KEYLEN> BucketT;

    // rows = m (probes per update), cols = d (buckets per row).
    Cadence(int rows, int cols, const Config& c = Config(), unsigned seed = 0xC0FFEE)
        : cfg(c), m_(rows), d_(cols), epoch_(0), rng_(seed), uni_(0.0, 1.0) {
        tbl_.resize((size_t)m_ * (size_t)d_);
        for (size_t i = 0; i < tbl_.size(); i++) Clear(tbl_[i]);
    }

    // -----------------------------------------------------------------
    // Algorithm 1: update with one arrival of `key` carrying weight `w`.
    // w = 1 is plain Cadence; any other w is the weighted variant
    // Cadence-W. Returns true if the key is resident and currently
    // satisfies all three gates; when it does and `out` is non-null, `out`
    // receives the descriptors the sketch holds for the key.
    // -----------------------------------------------------------------
    bool Update(const char* key, int w = 1, Report* out = NULL) {
        const int t = epoch_;
        int    best = -1;
        double bestScore = 1e18;

        for (int i = 0; i < m_; i++) {
            const size_t idx = (size_t)i * (size_t)d_ + (size_t)RowHash(key, i);
            BucketT& b = tbl_[idx];

            // Lazy rollover: the bucket still holds a stale epoch.
            if (b.occ && b.T != t) Commit(b, t);

            if (b.occ && memcmp(b.key, key, KEYLEN) == 0) {   // match
                b.C += w;
                if (!Qualified(b)) return false;
                if (out) {
                    out->key.assign(b.key, KEYLEN);
                    out->S = (int)b.S; out->mu = b.mu; out->mad = b.mad;
                }
                return true;
            }
            if (!b.occ) { Reinit(b, key, t, w); return false; }  // empty insertion

            // Collision candidate. A resident that currently satisfies all
            // three gates is protected and is never chosen as the victim.
            if (Qualified(b)) continue;
            const double s = Score(b);
            if (s < bestScore) { bestScore = s; best = (int)idx; }
        }

        // Every probed bucket was protected: the update is discarded.
        if (best < 0) return false;

        // Replacement: outright when the victim's score is non-positive,
        // otherwise with probability 2^(-lambda * score).
        if (bestScore <= 0.0 || uni_(rng_) < std::exp2(-cfg.lambda * bestScore))
            Reinit(tbl_[best], key, t, w);
        return false;
    }

    // -----------------------------------------------------------------
    // The per-epoch sweep of Algorithm 2. Commits the completed epoch in
    // every occupied bucket and advances the current epoch to t. Must be
    // called once per epoch boundary, before the first update of epoch t.
    // -----------------------------------------------------------------
    void NewEpoch(int t) {
        for (int e = epoch_ + 1; e <= t; e++)
            for (size_t i = 0; i < tbl_.size(); i++) {
                BucketT& b = tbl_[i];
                if (b.occ && b.T < e) Commit(b, e);
            }
        if (t > epoch_) epoch_ = t;
    }

    // -----------------------------------------------------------------
    // Algorithm 2: sweep, then report every resident that satisfies the
    // three gates. Subsumes NewEpoch(t).
    //
    // An item can briefly occupy a bucket in more than one row (it is
    // inserted into a row only while the earlier rows are occupied, and
    // an earlier row may fall empty later), so the report is deduplicated
    // on the key, keeping the longer-established copy.
    // -----------------------------------------------------------------
    void Query(int t, std::vector<Report>& out) {
        NewEpoch(t);
        out.clear();
        std::map<std::string, size_t> seen;
        for (size_t i = 0; i < tbl_.size(); i++) {
            const BucketT& b = tbl_[i];
            if (!b.occ || !Qualified(b)) continue;
            std::string k(b.key, KEYLEN);
            std::map<std::string, size_t>::iterator it = seen.find(k);
            if (it == seen.end()) {
                seen[k] = out.size();
                Report r; r.key = k; r.S = (int)b.S; r.mu = b.mu; r.mad = b.mad;
                out.push_back(r);
            } else if ((int)b.S > out[it->second].S) {
                Report& r = out[it->second];
                r.S = (int)b.S; r.mu = b.mu; r.mad = b.mad;
            }
        }
    }

    // -----------------------------------------------------------------
    // Durability. Runs that have already ended accumulate in DurableRuns();
    // this reports the runs still open at epoch t, as the provisional
    // interval [ts, t]. Deduplicate the two on the start tag ts.
    // -----------------------------------------------------------------
    void QueryDurable(int t, std::vector<Interval>& out) {
        NewEpoch(t);
        out.clear();
        std::map<std::string, size_t> seen;
        for (size_t i = 0; i < tbl_.size(); i++) {
            const BucketT& b = tbl_[i];
            if (!b.occ || (int)b.Q < cfg.L) continue;
            Interval iv;
            iv.key.assign(b.key, KEYLEN);
            iv.start = b.ts;
            iv.end   = t;
            std::map<std::string, size_t>::iterator it = seen.find(iv.key);
            if (it == seen.end()) { seen[iv.key] = out.size(); out.push_back(iv); }
            else if (iv.start < out[it->second].start) out[it->second] = iv;
        }
    }

    // Maximal runs of length >= L that have ended, in the order they ended.
    const std::vector<Interval>& DurableRuns() const { return runs_; }

    // End of stream: commit the final in-progress epoch and close every
    // run still open, so DurableRuns() is complete. Idempotent.
    void Flush() {
        NewEpoch(epoch_ + 1);
        for (size_t i = 0; i < tbl_.size(); i++) {
            BucketT& b = tbl_[i];
            if (!b.occ || (int)b.Q < cfg.L) continue;
            EmitRun(b);
            b.Q = 0; b.ts = -1;
        }
    }

    // The raw table, for callers that want to scan it themselves.
    const std::vector<BucketT>& Table() const { return tbl_; }

    int    Rows()        const { return m_; }
    int    Cols()        const { return d_; }
    int    Epoch()       const { return epoch_; }
    size_t BucketBytes() const { return sizeof(BucketT); }
    size_t MemoryBytes() const { return tbl_.size() * sizeof(BucketT); }

    Config cfg;

private:
    // ---- the three gates -------------------------------------------
    bool Qualified(const BucketT& b) const {
        return (int)b.S >= cfg.P
            && (cfg.muMin <= 0.0 || (double)b.mu >= cfg.muMin)
            && (double)b.mad <= cfg.tau;
    }

    // ---- retention score, Section III ------------------------------
    double Score(const BucketT& b) const {
        const double con = (double)b.S / (double)cfg.P;
        const double sig = (cfg.muMin > 0.0) ? (double)b.mu / cfg.muMin : 0.0;
        const double stb = (cfg.tau   > 0.0) ? (double)b.mad / cfg.tau  : 0.0;
        return con + sig - stb;
    }

    int RowHash(const char* key, int i) const {
        const uint64_t h = MurmurHash64B((const void*)key, KEYLEN,
                                         cfg.hashSeed + (unsigned)i * 0x9e3779b1u);
        return (int)(h % (uint64_t)d_);
    }

    // A fresh bucket starts with an empty streak and a mean seeded at
    // mu_min: the streak is 0 because no epoch has completed for the item,
    // and seeding the mean at the decision boundary keeps a newly inserted
    // item from being penalised in the replacement score before it has any
    // history.
    void Clear(BucketT& b) const {
        memset(b.key, 0, sizeof(b.key));
        b.T = -1; b.C = 0; b.mu = (float)cfg.muMin; b.mad = 0.0f;
        b.ts = -1; b.S = 0; b.Q = 0; b.occ = false;
    }

    void Reinit(BucketT& b, const char* key, int t, int w) const {
        memcpy(b.key, key, KEYLEN);
        b.T = t; b.C = w; b.mu = (float)cfg.muMin; b.mad = 0.0f;
        b.ts = -1; b.S = 0; b.Q = 0; b.occ = true;
    }

    void EmitRun(const BucketT& b) {
        Interval iv;
        iv.key.assign(b.key, KEYLEN);
        iv.start = b.ts;
        iv.end   = b.ts + (int)b.Q - 1;
        runs_.push_back(iv);
    }

    // -----------------------------------------------------------------
    // Commit the epoch held in b.C and re-tag the bucket to `next`.
    //
    //   continuity   the streak advances only if the committed epoch was
    //                consecutive with the previous one and was active
    //   descriptors  UpdateMeanMAD: the mean first, then the deviation
    //                against the just-updated mean
    //   durability   evaluate the three gates on the committed epoch and
    //                extend, start, or close the qualifying run
    // -----------------------------------------------------------------
    void Commit(BucketT& b, int next) {
        const int x = b.C;
        const int completed = b.T;

        if (next - b.T == 1 && x > 0) { if (b.S < 32767) b.S = (short)(b.S + 1); }
        else                          { b.S = 0; }

        const double mu = (1.0 - cfg.alpha) * (double)b.mu + cfg.alpha * (double)x;
        b.mu = (float)mu;
        const double dev = std::fabs((double)x - (double)b.mu);
        const double mad = (1.0 - cfg.alpha) * (double)b.mad + cfg.alpha * dev;
        b.mad = (float)mad;

        if (Qualified(b)) {
            if (b.Q == 0) b.ts = completed;
            if (b.Q < 32767) b.Q = (short)(b.Q + 1);
        } else {
            if ((int)b.Q >= cfg.L) EmitRun(b);
            b.Q = 0; b.ts = -1;
        }

        b.C = 0; b.T = next;
    }

    int                   m_, d_;
    int                   epoch_;
    std::vector<BucketT>  tbl_;
    std::vector<Interval> runs_;
    std::mt19937                           rng_;
    std::uniform_real_distribution<double> uni_;
};

}  // namespace cadence

#endif  // CADENCE_H
