#include <mutex>
#include <condition_variable>
#include <iostream>
#include <memory>
#include <cstring>
#include <deque>
#include <set>
#include <thread>
#include <functional>
#include <atomic>
#include <future>
#include <random>

#ifndef _AGM_PUB_
#define _AGM_PUB_

#ifdef  AGM_TEST
#define TEST_ONLY(expr) do { expr } while(0);
#else
#define TEST_ONLY(expr)
#endif

#define UNUSED(expr) do { (void)(expr); } while (0);
#define MILLISECONDS(ms) (std::chrono::milliseconds(ms))
#define NANOSECONDS(ms)  (std::chrono::nanoseconds(ms))
#define KB(n) (n * (0x1<<10))
#define MB(n) (n * (0x1<<20))
#define GB(n) (n * (0x1<<30))

#define OUT(...) do { \
    safe_print(string_format(__VA_ARGS__) + " at %s:%d ", __FILE__, __LINE__); \
} while(0);

#define ERR(...) do { \
    safe_print(std::string("ERR: ") + string_format(__VA_ARGS__) + " errno=%d(%s) at %s:%d", errno, std::strerror(errno), __FILE__, __LINE__); \
} while(0);

#define SLEEP_MS(ms) do { \
    std::this_thread::sleep_for(MILLISECONDS(ms)); \
} while(0);

#if defined(  __x86_64__ )
#define ALWAYS_BREAK()          __asm__("int $3")
#else
#define ALWAYS_BREAK()          ::abort()
#endif

#define AGM_FAULT_RATE (30)

inline static void __assertFunction( const char *message, const char * file, int line )
{
    std::cerr << "ASSERT:" << file << "(" << line << ") " << message << " " << errno << "(" << std::strerror(errno) << ") ";
    ALWAYS_BREAK();
}

#define ASSUME(must_be_true_predicate,msg)      \
    ((must_be_true_predicate)                   \
    ? (void )0                                  \
    : __assertFunction(msg,__FILE__,__LINE__))


std::mutex g_agm_mu_cout;

class agm_exception : public std::exception {
    int   _errno;
    std::string _errstr;
    std::string _whatmsg;
public:
    agm_exception(std::string msg) noexcept : _errno(errno), _errstr(std::strerror(errno))
    {
        _whatmsg = msg + "(errno(" + std::to_string(errno) + "): " + _errstr + ")";
    }
    const char* what() const noexcept {
        return _whatmsg.c_str();
    }
};

class timeout_exception : public std::runtime_error {
public:
  using std::runtime_error::runtime_error;
};

bool g_printtime = false;
void setprinttime(bool prt = false) {
    g_printtime = prt;
}

template<typename ... Args>
std::string string_format( const std::string& format, Args ... args )
{
    int size_s = std::snprintf( nullptr, 0, format.c_str(), args ... ) + 1; // Extra space for '\0'
    if( size_s <= 0 ){ throw std::runtime_error( "Error during formatting." ); }
    auto size = static_cast<size_t>( size_s );
    std::unique_ptr<char[]> buf( new char[size] );
    std::snprintf( buf.get(), size, format.c_str(), args ... );
    return std::string( buf.get(), buf.get() + size - 1 ); // We don't want the '\0' inside
}

template<typename ... Args>
void safe_print( std::string format, Args ... args )
{
    std::string t("");
    thread_local static std::chrono::steady_clock::time_point last_print_time = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> locker(g_agm_mu_cout);
    if(g_printtime) {
        t = string_format("(+%.4dms) ", (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - last_print_time)).count());
    }
    std::string s = t + string_format(format, args ... );
    std::cout << s << std::endl;
    last_print_time = std::chrono::steady_clock::now();
}

void errout(const char *msg = "") {
    safe_print( string_format("%s (%d: %s)", msg, errno, std::strerror(errno)) );
}

struct agm_lockable {
    std::recursive_mutex _agm_lockable_mu;
public:    
    lock() {
        OUT("agm_lockable LOCKING...");
        _agm_lockable_mu.lock();
    }
    unlock() noexcept {
        OUT("agm_lockable UNLOCKING...");
        _agm_lockable_mu.unlock();
    }
};


/** a timer class that tracks multiple timer events
 * 
 */
class agm_timer {
    using AgmTimerPtrType = std::shared_ptr<agm_timer>;
private:
    static std::thread *_pth;
    static std::mutex _mu;
    static std::set<AgmTimerPtrType> _timers;
    inline static void _ticktock() {
        safe_print("timer static thread running.");
        while(true) {
            std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
            auto itr = agm_timer::_timers.begin();
            while(itr != agm_timer::_timers.end()) {
                AgmTimerPtrType p = (*itr);
                if((now - p->_start) >= p->_dur ) {  // timeout
                    p->_func(p->_args);
                    if(p->_repeat) {
                        p->_start = std::chrono::steady_clock::now();
                        ++itr;
                    }
                    else {
                        itr = _uninstall(itr);
                    }
                }
                else {
                    ++itr;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
    static bool _install(AgmTimerPtrType & p) {
        std::lock_guard<std::mutex> lock(agm_timer::_mu);
        auto r = agm_timer::_timers.insert(p);
        return r.second;
    }
    static std::set<AgmTimerPtrType>::iterator _uninstall(const std::set<AgmTimerPtrType>::iterator & itr) {
        std::lock_guard<std::mutex> lock(agm_timer::_mu); 
        return agm_timer::_timers.erase(itr);
    }

    // instance variables
    std::chrono::milliseconds      _dur;           // timer duration
    std::function<void(void*)>     _func;          // callback function: void()
    void                           *_args;         // args for func
    bool                           _repeat;        // true repeat timer, else onetime timer
    std::chrono::steady_clock::time_point _start;  // timer starting time

    template<typename T>
    agm_timer(std::chrono::milliseconds ms, T&& task, void* parg, bool repeat=false) : _dur(ms),_repeat(repeat) {
        if (agm_timer::_pth == nullptr)
            agm_timer::_pth = new std::thread(agm_timer::_ticktock);
        _dur   = ms;
        _func  = std::move(task);
        _start = std::chrono::steady_clock::now();
        _args  = parg;
    }
public:
    agm_timer(const agm_timer& rhs) = delete;
    agm_timer& operator=(const agm_timer& rhs) = delete;
    agm_timer() = delete;
    
    /** 
     * install a new timer
     * return an 'agm_timer' object pointer
     */
    template<typename T>
    static AgmTimerPtrType install(std::chrono::milliseconds ms, T&& task, void* parg, bool repeat=false) {
        AgmTimerPtrType p(new agm_timer(ms, task, parg, repeat));
        _install(p);
        return p;
    }
    void reset() {
        _start = std::chrono::steady_clock::now();
    }
};

using AgmTimerPtrType = std::shared_ptr<agm_timer>;
std::set<AgmTimerPtrType> agm_timer::_timers;
std::thread *agm_timer::_pth = nullptr;
std::mutex agm_timer::_mu;

/** 
 * a circular buffer
 */
template <typename T>
class agm_circular {
private:
    std::mutex       _mu;
    std::condition_variable _cv;
    T                *_buf;
    std::atomic_int  _size;      // number of elements the buffer can contain
    std::atomic_int  _idx_read;  // tail of the buffer
    std::atomic_int  _idx_write; // head of the buffer
    std::atomic_bool _full;      // true: full, false: not full
public:
    agm_circular() = delete;
    agm_circular(int size) : _idx_read(0),_idx_write(0),_size(size),_buf(nullptr),_full(false) {
        _buf = new T[size];
    }
    agm_circular(const agm_circular &o) {
        agm_circular(o._size);
        // deep copy, T must support 'copy' operator '='
        for(int i=0; i<_size; i++) {
            _buf[i] = o._buf[i];
        }
        _size      = o._size;
        _idx_read  = o._idx_read;
        _idx_write = o._idx_write;
    }
    agm_circular(agm_circular&& o) {
        _buf       = o._buf; 
        _size      = o._size;
        _idx_read  = o._idx_read;
        _idx_write = o._idx_write;
        o._buf     = nullptr;       // move semantec
        o._size    = 0;             // move semantec
    }
    agm_circular& operator=(const agm_circular &o) {
        if (this == &o) {
            return *this;
        }
        if (_size != o._size) {
            delete[] _buf;
            _buf = new T[o._size];
        }
        _size      = o._size;
        _idx_read  = o._idx_read;
        _idx_write = o._idx_write;
        for(int i=0; i<_size; i++) {
            _buf[i] = o._buf[i];
        }
        return *this;
    }
    T& operator[](int i) { return _buf[i]; }

    ~agm_circular() {
        delete[] _buf;
    }

    bool write(const T& o) {
        if(_full)
            return false;
        std::lock_guard<std::mutex> locker(_mu);
        _buf[_idx_write] = o;
        _idx_write = (++_idx_write) % _size;
        if(_idx_write == _idx_read)
            _full = true;
        _cv.notify_one();
        return true;
    }
    bool write(T&& o) {
        if(_full)
            return false;
        std::lock_guard<std::mutex> locker(_mu);
        _buf[_idx_write] = std::move(o);
        _idx_write = (++_idx_write) % _size;
        if(_idx_write == _idx_read)
            _full = true;
        _cv.notify_one();
        return true;
    }
    bool write(const T* op) {
        if(_full)
            return false;
        std::lock_guard<std::mutex> locker(_mu);
        _buf[_idx_write] = *(op);
        _idx_write = (++_idx_write) % _size;
        if(_idx_write == _idx_read)
            _full = true;
        _cv.notify_one();
        return true;
    }
    
    T* read(int timeout=-1) {
        if(is_empty() && timeout==-1) {  // empty condition without timeout
            return nullptr;
        }
        std::unique_lock<std::mutex> locker(_mu);
        if(is_empty()) {
            bool rc;
            if(timeout) {    // timeout > 0
                rc = _cv.wait_for(locker, std::chrono::milliseconds(timeout), [this](){return !is_empty();} );
                if(!rc) {   // timeout and still empty
                    return nullptr;
                }
            }
            else {  // timeout == 0, wait forever
                _cv.wait(locker, [this](){return !is_empty();} );
            }
        }
        T* op = &(_buf[_idx_read]);
        _idx_read = (++_idx_read) % _size;
        _full = false;
        return op;
    }
    std::pair<T*,int> readpair(int timeout=-1) {
        if(is_empty() && timeout==-1) {  // empty condition without timeout
            return std::pair<T*, int>(nullptr, -1);
        }
        std::unique_lock<std::mutex> locker(_mu);
        if(is_empty()) {
            bool rc;
            if(timeout) {    // timeout > 0
                rc = _cv.wait_for(locker, std::chrono::milliseconds(timeout), [this](){return !is_empty();} );
                if(!rc) {   // timeout and still empty
                    return std::pair<T*, int>(nullptr, -1);
                }
            }
            else {  // timeout == 0, wait forever
                _cv.wait(locker, [this](){return !is_empty();} );
            }
        }
        int idx = _idx_read;
        T* op = &(_buf[idx]);
        _idx_read = (++_idx_read) % _size;
        _full = false;
        return std::pair<T*, int>(op, idx);
    }

    /** get current element without forward the index */
    T* current() {
        return &(_buf[_idx_read]);
    }
    /** move the read index forward by 's'
     * the read index won't move across the write index
     * if reaches write idx, stop stepping
     * return value: actual steps forwarded
     */
    int r_step(int s = 1) {
        int i = 0;
        if(_idx_read == _idx_write && !_full) {  // empty condition
            return 0;
        }
        for(i=0;i<s;i++) {
            _idx_read = (_idx_read+1) % _size;
            if(_idx_read == _idx_write)     // empty now
                return i+1;
        }
        return i;
    }
    /** move the write index forward
     * 
     */
    int w_step(int s = 1) {
        int i = 0;
        if(_full) {  // empty condition
            return 0;
        }
        std::lock_guard<std::mutex> locker(_mu);
        for(i=0;i<s;i++) {
            _idx_write = (++_idx_write) % _size;
            if(_idx_read == _idx_write) {     // full now
                _full = true;
                return i+1;
            }
        }
        return i;
    }

    inline bool is_empty() {
        return (_idx_write == _idx_read && !_full);
    }
    inline bool is_full() {
        return _full;
    }
};

class agm_singleton {
public:
    agm_singleton() = default;
    agm_singleton(const agm_singleton & s) = delete;
    agm_singleton & operator =(const agm_singleton & s) = delete;
    agm_singleton & getInstance() {
        static agm_singleton instance;
        return instance;
    }
    virtual ~agm_singleton() {}
};

template<typename T>
inline void agm_flag_set(T& bits, const T& flag) {
    bits |= flag;
}
template<typename T>
inline void agm_flag_unset(T& bits, const T& flag) {
    bits &= ~flag;
}
template<typename T>
inline bool agm_flag_is_set(const T& bits, const T& flag) {
    return (bits & flag);
}

    /**
     * @brief random funciton.
     * 
     * @param pct the probability of returning true (in percentage)
     * @return true if hit, else false
     */
    bool dice(int pct=0) {
        static unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
        static std::default_random_engine generator(seed);
        std::uniform_int_distribution<int> distribution(0,99);
        int number = distribution(generator);
        // OUT("uniform distribution number=%d", number);
        return (number < pct);
    }

#endif
