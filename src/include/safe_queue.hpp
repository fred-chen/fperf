#include <pub.hpp>
#include <atomic>
#include <cassert>
#include <cstdlib>
#include <memory>
#include <stdexcept>
#include <type_traits>
#include <utility>

template <typename T>
class SafeQueueInterface {
protected:
    int _maxsize;
public:
    /* pure virtual */
    virtual void   push(T item, int timeout=0) = 0;
    virtual T      pop(int timeout=0)          = 0;
    virtual bool   empty()                     = 0;
    virtual size_t size()                      = 0;
    virtual size_t capacity() const            = 0;
    virtual int    drain(int timeout=0)        = 0;

    /* public shared funtions */
    SafeQueueInterface(size_t size=1000) : _maxsize(size) {
    }
    /**
     * @brief wait for new element
     * 
     * @param timeout the duration for wait
     * @return true if queue is not empty before timeout
     * @return false if queue is still empty after timeout
     */
};

template <typename T>
class safe_queue : public SafeQueueInterface<T> {
    using TaskType = T;
    using QueueType = std::deque<T>;
private:
    std::mutex _m;
    QueueType _q;
    std::condition_variable _cv;
public:
    safe_queue(size_t size=1000) : SafeQueueInterface<T>(size) {}
    void push(TaskType item, int timeout=0) {
        std::unique_lock<std::mutex> locker(_m);
        if(_q.size()>=this->_maxsize) {
            // q is full, block until available slots
            if(!timeout) {
                _cv.wait(locker, [this](){return _q.size()<this->_maxsize;});
            }
            else {
                if(!_cv.wait_for(locker, MILLISECONDS(timeout), 
                        [this](){return _q.size()<this->_maxsize;}))
                {
                    // timeout but still no available slot
                    throw timeout_exception("timeout waiting on full queue.");
                }
            }
        }
        _q.push_back(item);
        _cv.notify_one();
    }
    void push_front(TaskType item) {
        std::lock_guard<std::mutex> locker(_m);
        _q.push_front(item);
        _cv.notify_one();
    }
    TaskType pop(int timeout=0) {
        std::unique_lock<std::mutex> locker(_m);
        if(_q.empty()) {
            if(!timeout) {
                _cv.wait(locker, [this](){return !_q.empty();});
            }
            else {
                if(!_cv.wait_for(locker, MILLISECONDS(timeout), [this](){return !_q.empty();})) {
                    throw timeout_exception("timeout waiting on empty queue.");
                }
            }
        }
        TaskType item = _q.front();
        _q.pop_front();
        _cv.notify_one();
        return item;
    }
    TaskType front() {
        std::unique_lock<std::mutex> locker(_m);
        return _q.front();
    }
    TaskType back() {
        std::unique_lock<std::mutex> locker(_m);
        return _q.back();
    }
    TaskType operator[] (int idx) {
        std::unique_lock<std::mutex> locker(_m);
        return _q[idx];
    }
    TaskType pop_back(int timeout=0) {
        std::unique_lock<std::mutex> locker(_m);
        if(_q.empty()) {
            if(!timeout) {
                _cv.wait(locker, [this](){return !_q.empty();});
            }
            else {
                if(!_cv.wait_for(locker, MILLISECONDS(timeout), [this](){return !_q.empty();})) {
                    throw timeout_exception("timeout waiting on empty queue.");
                }
            }
        }
        TaskType item = _q.back();
        _q.pop_back();
        _cv.notify_one();
        return item;
    }
    bool empty() {
        std::lock_guard<std::mutex> locker(_m);
        return _q.empty();
    }
    size_t size() {
        std::lock_guard<std::mutex> locker(_m);
        return _q.size();
    }
    
    size_t capacity() const {
      return 0;
    }

    void wait() {
        std::unique_lock<std::mutex> locker(_m);
        if(_q.empty()) {
            _cv.wait( locker, [this](){return !_q.empty();} );
        }
    }
    void lock() {
        _m.lock();
    }
    void unlock() {
        _m.unlock();
    }
    bool trylock() {
        return _m.try_lock();
    }
    /**
     * @brief wait for new element
     * 
     * @param timeout the duration for wait
     * @return true if queue is not empty before timeout
     * @return false if queue is still empty after timeout
     */
    bool wait_for(const int ms) {
        std::unique_lock<std::mutex> locker(_m);
        return _cv.wait_for( locker, MILLISECONDS(ms), [this](){return !_q.empty();} );
    }
    bool wait_for(const std::chrono::milliseconds &timeout) {
        std::unique_lock<std::mutex> locker(_m);
        if(_q.empty()) {
            return _cv.wait_for( locker, timeout, [this](){return !_q.empty();} );
        }
        return true;
    }

    /**
     * @brief wait until empty or timeout
     * 
     * @param timeout timeout
     * @return int return the number of remaining elements if timeout, or 0 if empty
     */
    int drain(int timeout=0) {
        std::unique_lock<std::mutex> locker(_m);

        if(!_q.empty()) {
            if(!timeout) {
                _cv.wait( locker, [this](){return _q.empty();});
            }
            else {
                if(!_cv.wait_for( locker, MILLISECONDS(timeout), [this](){return _q.empty();})) {
                    // timeout
                    return _q.size();
                }
            }
        }
        return _q.size();
    }
};

/*
 * LockfreeQueue is a one producer and one consumer queue
 * without locks.
 */
template <class T>
struct LockfreeQueue : public SafeQueueInterface<T> {
    typedef T value_type;

    LockfreeQueue(const LockfreeQueue&) = delete;
    LockfreeQueue& operator=(const LockfreeQueue&) = delete;

    // size must be >= 2.
    //
    // Also, note that the number of usable slots in the queue at any
    // given time is actually (size-1), so if you start with an empty queue,
    // isFull() will return true after size-1 insertions.
    explicit LockfreeQueue(size_t size = 1000)
        : SafeQueueInterface<T>(size),
            records_(static_cast<T*>(std::malloc(sizeof(T) * size))),
            readIndex_(0),
            writeIndex_(0) {
        assert(size >= 2);
        if (!records_) {
        throw std::bad_alloc();
        }
    }

    ~LockfreeQueue() {
        // We need to destruct anything that may still exist in our queue.
        // (No real synchronization needed at destructor time: only one
        // thread can be doing this.)
        if (!std::is_trivially_destructible<T>::value) {
        size_t readIndex = readIndex_;
        size_t endIndex = writeIndex_;
        while (readIndex != endIndex) {
            records_[readIndex].~T();
            if (++readIndex == this->_maxsize) {
            readIndex = 0;
            }
        }
        }

        std::free(records_);
    }

    template <class... Args>
    bool write(Args&&... recordArgs) {
        auto const currentWrite = writeIndex_.load(std::memory_order_relaxed);
        auto nextRecord = currentWrite + 1;
        if (nextRecord == this->_maxsize) {
        nextRecord = 0;
        }
        if (nextRecord != readIndex_.load(std::memory_order_acquire)) {
        new (&records_[currentWrite]) T(std::forward<Args>(recordArgs)...);
        writeIndex_.store(nextRecord, std::memory_order_release);
        return true;
        }

        // queue is full
        return false;
    }
    void push(T item, int timeout=0) {
        auto start = std::chrono::steady_clock::now();
        auto dur   = MILLISECONDS(0);
        bool ret = false;

        while(!(ret = write(std::forward<T>(item))) &&
              (!timeout || dur <= MILLISECONDS(timeout))) 
        {
            if(timeout)
                dur = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now()-start);
        }

        if(!ret)
            throw timeout_exception("timeout waiting on full queue.");
    }

    // move (or copy) the value at the front of the queue to given variable
    bool read(T& record) {
        auto const currentRead = readIndex_.load(std::memory_order_relaxed);
        if (currentRead == writeIndex_.load(std::memory_order_acquire)) {
        // queue is empty
        return false;
        }

        auto nextRecord = currentRead + 1;
        if (nextRecord == this->_maxsize) {
        nextRecord = 0;
        }
        record = std::move(records_[currentRead]);
        records_[currentRead].~T();
        readIndex_.store(nextRecord, std::memory_order_release);
        return true;
    }

    T pop(int timeout=0) {
        auto start = std::chrono::steady_clock::now();
        auto dur   = MILLISECONDS(0);
        T    record;

        while(!timeout || dur <= MILLISECONDS(timeout)) {
            if(read(record)) {
                return record;
            }
            if(timeout)
                dur = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now()-start);
        }

        throw timeout_exception("timeout waiting on empty queue.");
    }

    // pointer to the value at the front of the queue (for use in-place) or
    // nullptr if empty.
    T* frontPtr() {
        auto const currentRead = readIndex_.load(std::memory_order_relaxed);
        if (currentRead == writeIndex_.load(std::memory_order_acquire)) {
        // queue is empty
        return nullptr;
        }
        return &records_[currentRead];
    }

    T front() {
        T* record_ptr = frontPtr();
        if(!record_ptr) {
        throw std::out_of_range("failed accessing front item.");
        }
        return *record_ptr;
    }

    // queue must not be empty
    void popFront() {
        auto const currentRead = readIndex_.load(std::memory_order_relaxed);
        assert(currentRead != writeIndex_.load(std::memory_order_acquire));

        auto nextRecord = currentRead + 1;
        if (nextRecord == this->_maxsize) {
        nextRecord = 0;
        }
        records_[currentRead].~T();
        readIndex_.store(nextRecord, std::memory_order_release);
    }

    bool isEmpty() const {
        return readIndex_.load(std::memory_order_acquire) ==
            writeIndex_.load(std::memory_order_acquire);
    }
    bool empty() {
        return isEmpty();
    }

    /**
     * @brief wait until empty or timeout
     * 
     * @param timeout timeout in milliseconds
     * @return int return the number of remaining elements if timeout, or 0 if empty
     */
    int drain(int timeout=0) {
        auto start = std::chrono::steady_clock::now();
        auto dur   = MILLISECONDS(0);

        while(!isEmpty() && ( !timeout || dur <= MILLISECONDS(timeout))) {
            if(timeout)
                dur = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now()-start);
        }

        return size();
    }

    bool isFull() const {
        auto nextRecord = writeIndex_.load(std::memory_order_acquire) + 1;
        if (nextRecord == this->_maxsize) {
        nextRecord = 0;
        }
        if (nextRecord != readIndex_.load(std::memory_order_acquire)) {
        return false;
        }
        // queue is full
        return true;
    }
    bool full() const {
        return isFull();
    }

    // * If called by consumer, then true size may be more (because producer may
    //   be adding items concurrently).
    // * If called by producer, then true size may be less (because consumer may
    //   be removing items concurrently).
    // * It is undefined to call this from any other thread.
    size_t sizeGuess() const {
        int ret = writeIndex_.load(std::memory_order_acquire) -
            readIndex_.load(std::memory_order_acquire);
        if (ret < 0) {
        ret += this->_maxsize;
        }
        return ret;
    }
    size_t size() {
        return sizeGuess();
    }

    // maximum number of items in the queue.
    size_t capacity() const { return this->_maxsize - 1; }

    private:
    using AtomicIndex = std::atomic<unsigned int>;
    T* const records_;

    AtomicIndex readIndex_;
    AtomicIndex writeIndex_;
};

#define SafeQueueType safe_queue