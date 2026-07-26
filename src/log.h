#ifndef LOG_H
#define LOG_H

#include <iostream>
#include <sstream>
#include <mutex>

namespace loginternal {
// Single process-wide mutex guarding emission to std::cout.
inline std::mutex& cout_mutex() {
    static std::mutex m;
    return m;
}
}  // namespace loginternal

// Compile-time trace switch. Traces are OFF by default; build with -DPAXOS_TRACE
// (e.g. cmake -DPAXOS_TRACE=ON) to compile them in. Because this is a constexpr
// constant, `if constexpr (kTrace)` lets the compiler physically strip every
// disabled LOG statement -- no branch, no LogLine, no string literals emitted.
inline constexpr bool kTrace =
#ifdef PAXOS_TRACE
    true;
#else
    false;
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
        std::lock_guard<std::mutex> lock(loginternal::cout_mutex());
        std::cout << oss_.str();
        std::cout.flush();
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
