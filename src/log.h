#ifndef LOG_H
#define LOG_H

#include <iostream>
#include <sstream>
#include <mutex>
#include <atomic>
#include <cstring>
#include <unistd.h>

namespace loginternal {
// Single process-wide mutex guarding emission to std::cout.
inline std::mutex& cout_mutex() {
    static std::mutex m;
    return m;
}

// ---------------------------------------------------------------------------
// Ring-buffer trace (PAXOS_TRACE_RING)
//
// Writing every LOG statement to stdout costs a process-wide mutex plus a write
// syscall, hundreds of times per transaction. That serialises a node's threads and
// slows it enough to change which interleavings occur -- so turning tracing on to
// diagnose a bug can stop the bug happening.
//
// This keeps the trace in memory instead: each statement is formatted into a
// preallocated fixed-size slot in a circular buffer, published with one relaxed
// atomic increment. No lock, no allocation, no syscall. The buffer is dumped only
// on SIGUSR1 or SIGHUP/SIGTERM (see node_launch.cpp), so a
// passing run pays formatting cost only and a failing run still yields the last
// kRingSlots statements.
// ---------------------------------------------------------------------------
constexpr size_t kRingSlots   = 300000;   // statements retained (temp bump for a 4500-txn repro)
constexpr size_t kRingSlotLen = 200;    // bytes per statement, truncated beyond

inline char (&ring_buf())[kRingSlots][kRingSlotLen] {
    static char buf[kRingSlots][kRingSlotLen] = {};
    return buf;
}
inline std::atomic<unsigned long long>& ring_idx() {
    static std::atomic<unsigned long long> i{0};
    return i;
}

inline void ring_write(const char* data, size_t n) {
    unsigned long long slot = ring_idx().fetch_add(1, std::memory_order_relaxed);
    char* dst = ring_buf()[slot % kRingSlots];
    if (n >= kRingSlotLen) n = kRingSlotLen - 1;
    std::memcpy(dst, data, n);
    dst[n] = '\0';
}

// Async-signal-safe: only write(). Safe to call from a signal handler.
inline void ring_dump(int fd) {
    static const char hdr[] = "\n=== TRACE RING (oldest -> newest) ===\n";
    ssize_t rc = ::write(fd, hdr, sizeof(hdr) - 1); (void)rc;
    unsigned long long end = ring_idx().load(std::memory_order_relaxed);
    unsigned long long begin = (end > kRingSlots) ? end - kRingSlots : 0;
    for (unsigned long long k = begin; k < end; ++k) {
        const char* p = ring_buf()[k % kRingSlots];
        size_t len = ::strnlen(p, kRingSlotLen);
        if (len) { rc = ::write(fd, p, len); (void)rc; rc = ::write(fd, "\n", 1); (void)rc; }
    }
    static const char ftr[] = "=== END TRACE RING ===\n";
    rc = ::write(fd, ftr, sizeof(ftr) - 1); (void)rc;
}
}  // namespace loginternal

// Compile-time trace switch. Traces are OFF by default; build with -DPAXOS_TRACE
// (e.g. cmake -DPAXOS_TRACE=ON) to compile them in. Because this is a constexpr
// constant, `if constexpr (kTrace)` lets the compiler physically strip every
// disabled LOG statement -- no branch, no LogLine, no string literals emitted.
inline constexpr bool kTrace =
#if defined(PAXOS_TRACE) || defined(PAXOS_TRACE_RING)
    true;
#else
    false;
#endif

// When set, LOG statements are captured in the in-memory ring instead of being
// written to stdout. Compiled in, but costs no lock and no syscall.
inline constexpr bool kTraceRing =
#ifdef PAXOS_TRACE_RING
    true;
#else
    false;
#endif

// Compile-time PrintLog switch. Benchmark builds (-DPAXOS_BENCHMARK) strip every
// print_log_ append, together with its printlog_mutex_ acquisition and the
// accept_log_ snapshot taken under log_mutex_ to build it. PrintLog then returns
// a placeholder instead of the protocol message history.
inline constexpr bool kPrintLog =
#ifdef PAXOS_BENCHMARK
    false;
#else
    true;
#endif

// Thread-safe line logger.
//
// Accumulates one full statement into a local ostringstream and emits it to
// std::cout as a single mutex-protected write in the destructor. Concurrent
// `std::cout << ...` formatting from multiple threads is a data race on the
// shared stream buffer (undefined behavior) and was corrupting the heap; routing
// every log statement through a LogLine temporary makes each statement an atomic,
// non-interleaved write instead.
//
// Usage is a drop-in for std::cout:  LOG << "x=" << x << std::endl;
class LogLine {
public:
    LogLine() = default;
    LogLine(const LogLine&) = delete;
    LogLine& operator=(const LogLine&) = delete;

    ~LogLine() {
        if constexpr (kTraceRing) {
            // In-memory only: no mutex, no syscall, so node threads are not serialised.
            const std::string s = oss_.str();
            loginternal::ring_write(s.data(), s.size());
        } else {
            std::lock_guard<std::mutex> lock(loginternal::cout_mutex());
            std::cout << oss_.str();
            std::cout.flush();
        }
    }

    template <typename T>
    LogLine& operator<<(const T& value) {
        oss_ << value;
        return *this;
    }

    // Support stream manipulators such as std::endl and std::flush.
    LogLine& operator<<(std::ostream& (*manip)(std::ostream&)) {
        oss_ << manip;
        return *this;
    }

private:
    std::ostringstream oss_;
};

// Trace logging: compiled out entirely unless PAXOS_TRACE is defined.
// The inverted `!kTrace {} else` form is dangling-else safe, so a following
// `else` binds to the caller's `if`, not to this macro.
#define LOG if constexpr (!kTrace) {} else LogLine()

// Error logging: always compiled in, regardless of PAXOS_TRACE, so real
// failures still surface in a no-trace (benchmark/default) build.
#define LOGERR LogLine()

#endif  // LOG_H
