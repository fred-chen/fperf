/**
 * @author Fred
 * @email  fred.chen@live.com
 * @create date 2021-10-05 17:01:23
 * @modify date 2021-10-05 17:01:23
 */
#pragma once


#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <ifaddrs.h>
#include <linux/if_packet.h>
#include <net/ethernet.h>
#include <assert.h>
#include <netdb.h>
#include <string.h>
#include <sys/fcntl.h>
#include <sys/poll.h>
#include <string>
#include <string.h>
#include <climits>
#include <arpa/inet.h>
#include <map>
#include <safe_queue.hpp>
#include <pub.hpp>

#ifdef AGM_RDMA
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <rdma/rsocket.h>
#endif


#define RECVER_PORT  ("28444")
#define CTRL_PORT    ("22000")

#define CTLMSG_CLOSE (0x1 << 0)

#define QTLOCKFREE   (0)       // queue type lockfree
#define QTMUTEX      (1)       // queue type mutex

#define PROTTCP    SOCK_STREAM // tcp  protocol
#define PROTUDP    SOCK_DGRAM  // udp  protocol
#define PROTRDMA   (0)         // rdma protocol

#define MAX_SOCKS  (100)       // max nsocks for a single stream

/** data **/

using std::string;
using std::atomic_uint_fast64_t;

enum F_ERROR {
    FENONE = 0,            // no error
    FEACCEPT  = 1,         // failed sock_ops->recvfrom() in UDP stream
    FETIMEOUT = 2,         // timeout
};
extern int f_errno = FENONE;

struct fsettings_t {
    int    nstreams    = 1;                    // number of streams
    int    nsockets    = 1;                    // number of sockets
    size_t pktsize     = KB(32);               // packet size, 32KB default
    bool   roundtrip   = false;                // wait for ack before sending next?
    char   ip[46]      = {0};                  // ip address of server
    char   rdmadev[20] = {0};                  // rdma device to use
    int    opcode      = 0;                    // operation code. 0 server, 1 client
    size_t size        = MB(1)*size_t(10240);  // default size to send, 10GB
    int    prot        = PROTTCP;              // protocol, PROTTCP or PROTUDP, default PROTTCP
    int    qtype       = QTMUTEX;              // choose queue type for streams, default QTMUTEX
    bool   nodelay     = false;                // for TCP only, add TCP_NODELAY flag to disable nagle's algorithm
    int    quickack    = false;                // for TCP only, add TCP_QUICKACK flag to disable delayed ack algorithm
};
/*
 * application settings 
 */
struct fsettings {
    struct fsettings_t settings;
    operator string () {
        return string("ip        = ") + string(settings.ip?settings.ip:"''")                   + "\n" +
               string("prot      = ") + string(settings.prot==PROTTCP?"TCP":"UDP")             + "\n" +
               string("size      = ") + std::to_string(settings.size/1024/1024) + string("MB") + "\n" +
               string("pktsize   = ") + std::to_string(settings.pktsize)        + string("B")  + "\n" +
               string("qtype     = ") + string(settings.qtype==QTMUTEX?"MUTEX":"LOCKFREE")     + "\n" +
               string("nstreams  = ") + std::to_string(settings.nstreams)                      + "\n" +
               string("nsockets  = ") + std::to_string(settings.nsockets)                      + "\n" +
               string("roundtrip = ") + string(settings.roundtrip?"TRUE":"FALSE")              + "\n" +
               string("opcode    = ") + std::to_string(settings.opcode)                        + "\n";
               string("nodelay   = ") + string(settings.nodelay?"TRUE":"FALSE")                + "\n";
               string("quickack  = ") + string(settings.quickack?"TRUE":"FALSE")               + "\n";
    }
};

/*
 * buf structure to add in the queue 
 */
struct fbuf_t {
    char*  buf;
    int    len;
    int    timeout;
};

/*
 * statistic matrix
 */
struct fstat {
    atomic_uint_fast64_t max_lat_nano;     // maximum latency in nano seconds
    atomic_uint_fast64_t min_lat_nano;     // minimum latency in nano seconds
    atomic_uint_fast64_t avg_lat_nano;     // average latency in nano seconds
    atomic_uint_fast64_t tot_lat_nano;     // sum of latencies in nano seconds
    atomic_uint_fast64_t op_count;         // operation count in nano seconds
    string               op_name;          // operation name
    atomic_uint_fast64_t nsent;            // bytes sent
    atomic_uint_fast64_t nrecv;            // bytes received
    fstat() {
        max_lat_nano.store(0);
        min_lat_nano.store(INT_MAX); 
        avg_lat_nano.store(0);
        tot_lat_nano.store(0);
        op_count.store(0);
        nsent.store(0);
        nrecv.store(0);
        op_name = "";
    }
    fstat(const char *opname) {
        max_lat_nano.store(0);
        min_lat_nano.store(INT_MAX); 
        avg_lat_nano.store(0);
        tot_lat_nano.store(0);
        op_count.store(0);
        nsent.store(0);
        nrecv.store(0);
        op_name = opname;
    }
    fstat( const struct fstat & stat) {
        max_lat_nano.store(stat.max_lat_nano);
        min_lat_nano.store(stat.min_lat_nano); 
        avg_lat_nano.store(stat.avg_lat_nano);
        tot_lat_nano.store(stat.tot_lat_nano);
        op_count.store(stat.op_count);
        nsent.store(stat.nsent);
        nrecv.store(stat.nrecv);
        op_name = stat.op_name;
    }
    operator string () {
        return string("max_lat_nano: ") + std::to_string(max_lat_nano) + " " +
               string("min_lat_nano: ") + std::to_string(min_lat_nano) + " " +
               string("avg_lat_nano: ") + std::to_string(avg_lat_nano) + " " +
               string("tot_lat_nano: ") + std::to_string(tot_lat_nano) + " " +
               string("op_count: ")     + std::to_string(op_count)     + " " +
               string("nsent: ")        + std::to_string(op_count)     + " " +
               string("nrecv: ")        + std::to_string(op_count)     + " " +
               string("op_name: ")      + op_name;
    }
};


struct socket_ops_t {
    int (*socket)(int domain, int type, int protocol);
    int (*bind)(int socket, const struct sockaddr *addr, socklen_t addrlen);
    int (*listen)(int socket, int backlog);
    int (*accept)(int socket, struct sockaddr *addr, socklen_t *addrlen);
    int (*connect)(int socket, const struct sockaddr *addr, socklen_t addrlen);
    int (*shutdown)(int socket, int how);
    int (*close)(int socket);

    ssize_t (*recv)(int socket, void *buf, size_t len, int flags);
    ssize_t (*recvfrom)(int socket, void *buf, size_t len, int flags,
            struct sockaddr *src_addr, socklen_t *addrlen);
    ssize_t (*recvmsg)(int socket, struct msghdr *msg, int flags);
    ssize_t (*send)(int socket, const void *buf, size_t len, int flags);
    ssize_t (*sendto)(int socket, const void *buf, size_t len, int flags,
            const struct sockaddr *dest_addr, socklen_t addrlen);
    ssize_t (*sendmsg)(int socket, const struct msghdr *msg, int flags);
    ssize_t (*read)(int socket, void *buf, size_t count);
    ssize_t (*readv)(int socket, const struct iovec *iov, int iovcnt);
    ssize_t (*write)(int socket, const void *buf, size_t count);
    ssize_t (*writev)(int socket, const struct iovec *iov, int iovcnt);

    int (*poll)(struct pollfd *fds, nfds_t nfds, int timeout);
    int (*select)(int nfds, fd_set *readfds, fd_set *writefds,
            fd_set *exceptfds, struct timeval *timeout);

    int (*getpeername)(int socket, struct sockaddr *addr, socklen_t *addrlen);
    int (*getsockname)(int socket, struct sockaddr *addr, socklen_t *addrlen);

    ssize_t (*freadn)(int socket, char *buf, size_t count);
    ssize_t (*fwriten)(int socket, const char *buf, size_t count);
    int (*getsockdomain)(int sock);

    int (*setsockopt)(int socket, int level, int optname,
            const void *optval, socklen_t optlen);
    int (*getsockopt)(int socket, int level, int optname,
            void *optval, socklen_t *optlen);
    int (*fcntl)(int socket, int cmd, ... /* arg */ );
};




/** functions **/

/**
 * @brief read n bytes from fd
 * 
 * @param fd     the fd to read from
 * @param buf    buffer to receive bytes
 * @param count  buffer size
 * @return int   number of bytes read
 */
ssize_t freadn(int fd, char *buf, size_t count)
{
    register ssize_t r;
    register size_t nleft = count;

    while (nleft > 0) {
        r = ::read(fd, buf, nleft);
        if (r < 0) {
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)
                break;
            else
                return -1;
        } else if (r == 0)
            break;

        nleft -= r;
        buf   += r;
    }
    return count - nleft;
}


/**
 * @brief write n bytes to fd
 * 
 * @param fd     the fd to write to
 * @param buf    buffer contains bytes
 * @param count  data size
 * @return int   number of bytes written
 */
ssize_t fwriten(int fd, const char *buf, size_t count)
{
    register ssize_t r;
    register size_t nleft = count;
    while (nleft > 0) {
        r = ::write(fd, buf, nleft);
        if (r < 0) {
            switch (errno) {
                case EINTR : 
                case EAGAIN:
#if (EAGAIN != EWOULDBLOCK)
                case EWOULDBLOCK:
#endif                
                    return count - nleft;
                case ENOBUFS:
                default:
                    return -1;
            }
        }
        else if (r == 0) {
            return -1;
        }
        nleft -= r;
        buf   += r;
    }
    return count;
}

#ifdef AGM_RDMA
/**
 * @brief read n bytes from fd
 * 
 * @param fd     the fd to read from
 * @param buf    buffer to receive bytes
 * @param count  buffer size
 * @return int   number of bytes read
 */
ssize_t rfreadn(int fd, char *buf, size_t count)
{
    register ssize_t r;
    register size_t nleft = count;

    while (nleft > 0) {
        r = ::rread(fd, buf, nleft);
        if (r < 0) {
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)
                break;
            else
                return -1;
        } else if (r == 0)
            break;

        nleft -= r;
        buf   += r;
    }
    return count - nleft;
}


/**
 * @brief write n bytes to fd
 * 
 * @param fd     the fd to write to
 * @param buf    buffer contains bytes
 * @param count  data size
 * @return int   number of bytes written
 */
ssize_t rfwriten(int fd, const char *buf, size_t count)
{
    register ssize_t r;
    register size_t nleft = count;
    while (nleft > 0) {
        r = ::riowrite(fd, buf, nleft, 0, 0);
        if (r < 0) {
            switch (errno) {
                case EINTR : 
                case EAGAIN:
#if (EAGAIN != EWOULDBLOCK)
                case EWOULDBLOCK:
#endif                
                    return count - nleft;
                case ENOBUFS:
                default:
                    return -1;
            }
        }
        else if (r == 0) {
            return -1;
        }
        nleft -= r;
        buf   += r;
    }
    return count;
}

int rgetsockdomain(int sock)
{
    struct sockaddr_storage sa;
    socklen_t len = sizeof(sa);

    if (::rgetsockname(sock, (struct sockaddr *)&sa, &len) < 0) {
        return -1;
    }
    return ((struct sockaddr *) &sa)->sa_family;
}
#endif

int getsockdomain(int sock)
{
    struct sockaddr_storage sa;
    socklen_t len = sizeof(sa);

    if (::getsockname(sock, (struct sockaddr *)&sa, &len) < 0) {
        return -1;
    }
    return ((struct sockaddr *) &sa)->sa_family;
}

#define TEST_STRING ("abcdefghijklmnopqrstuvwxyz")
char* longstr(size_t len) {
    char   *buf     = new char[len];        // will not delete because this is only a test
    size_t unit_len = strlen(TEST_STRING);
    size_t nleft    = len;
    char   *p       = buf;

    while (nleft!=0)
    {
        strncpy(p, TEST_STRING, unit_len > nleft ? nleft : unit_len);
        p     += unit_len;
        nleft -= unit_len > nleft ? nleft : unit_len;
    }
    *(buf+len-1) = '\0';

    return buf;
}

struct socket_ops_t socket_ops = {
    ::socket,
    ::bind,
    ::listen,
    ::accept,
    ::connect,
    ::shutdown,
    ::close,

    ::recv,
    ::recvfrom,
    ::recvmsg,
    ::send,
    ::sendto,
    ::sendmsg,
    ::read,
    ::readv,
    ::write,
    ::writev,

    ::poll,
    ::select,

    ::getpeername,
    ::getsockname,

    ::freadn,
    ::fwriten,
    ::getsockdomain,

    ::setsockopt,
    ::getsockopt,
    ::fcntl
};

#ifdef AGM_RDMA
struct socket_ops_t rsocket_ops = {
    ::rsocket,
    ::rbind,
    ::rlisten,
    ::raccept,
    ::rconnect,
    ::rshutdown,
    ::rclose,

    ::rrecv,
    ::rrecvfrom,
    ::rrecvmsg,
    ::rsend,
    ::rsendto,
    ::rsendmsg,
    ::rread,
    ::rreadv,
    ::rwrite,
    ::rwritev,

    ::rpoll,
    ::rselect,

    ::rgetpeername,
    ::rgetsockname,

    ::rfreadn,
    ::rfwriten,
    ::rgetsockdomain,

    ::rsetsockopt,
    ::rgetsockopt,
    ::rfcntl
};
#endif





/* objects */

using TimePoint    = std::chrono::high_resolution_clock::time_point;
using CounterClock = std::chrono::high_resolution_clock;
constexpr static TimePoint TPEpoch{};       // the 'zero' timepoint

/**
 * @brief stream classes that emulate a stream-like network transition
 * 
 */
class f_stream_interface {
public:
    int   prot;                                 // the protocol, either PROTTCP or PROTUDP
protected:
    struct socket_ops_t *sock_ops;              // socket functions
    char* lname;                                // local address str
    char* rname;                                // remote address str
    char* lport;                                // local port str
    char* rport;                                // remote port str
    int   _sock_listen;                         // the socket for accepting remote connection
    struct sockaddr local_addr, peer_addr;      // local address and remote address after binding and connect
    SafeQueueInterface<struct fbuf_t> *send_q;  // send q and recv q
    int _nsocks;                                // number of sockets
    int _queue_type;                            // queue type: QTLOCKFREE or QTMUTEX
    char _quit;                                 // control variable to tell every thread to quit
    std::vector<std:: thread*> _sender_threads;
    std::vector<int> _vsocks;
    std::atomic_char _nsending;                 // number of outstanding senders
    std::map<int, fstat> _sender_stats;         // per-socket stats for sender threads
    struct fstat         _sender_stat;          // overall sender stats
    struct fstat         _recver_stat;          // receiver stats
    struct fstat         _submit_stat;          // stats for submits
    struct fstat         _mprtt_stat;           // stats for submits
    bool                 _roundtrip;            // enable roundtrip (wait for response before send returns)
    struct pollfd        _pfd_in [MAX_SOCKS];   // the poll event fd for socket list, detecting POLLIN events
    struct pollfd        _pfd_out[MAX_SOCKS];   // the poll event fd for socket list, detecting POLLOUT events

// public:
//     std::atomic_size_t n_to_send;
//     std::atomic_size_t n_did_send;
//     std::atomic_size_t nerr;
//     std::atomic_size_t n_did_recve;

public:
    // pure virtual
    virtual int listen()                                             = 0;
    virtual struct addrinfo _addrhints_for_bind()                    = 0;
    virtual struct addrinfo _addrhints_for_conn()                    = 0;
    virtual int _accept(char *_rname, char* _rport)                  = 0;
    virtual int _connect(char *_rname, char* _rport, int timeout=-1) = 0;

    virtual int connect(int timeout=-1) {
        return connect(rname, rport, timeout);
    }
    virtual int connect(char *_rname, char* _rport, int timeout=-1) {
        int flags;
        int ret, on=1;
        struct addrinfo hints, *res = nullptr;
        struct sockaddr_storage sa;
        socklen_t addrlen = sizeof(struct sockaddr_storage);

        for(int i=0; i<_nsocks; i++) {
            int s;
            if((s = _connect(_rname, _rport, timeout))<0) {
                ERR("failed connecting to peer.");
                return -1;
            }
            if ((addfdflag(s, O_NONBLOCK)) == -1) {
                ERR("failed to set nonblock socket.");
                return -1;
            }
            _vsocks.push_back(s);
            // safe_print("new connection %s:%d <=> %s:%d.",
            //     str_local_address(s).c_str(), local_port(s), str_peer_address(s).c_str(), peer_port(s));
        }
        initpollfds();

        addrlen = sizeof(struct sockaddr);
        // get the bound address info and set
        if (sock_ops->getsockname(_vsocks[0], (struct sockaddr *) &sa, &addrlen) < 0) {
            ERR("failed to get socket name.");
            freeaddrinfo(res);
            return -1;
        }
        memcpy(&local_addr, &sa, addrlen);
        // set peer address since connect succeeded
        if (sock_ops->getpeername(_vsocks[0], (struct sockaddr *) &sa, &addrlen) < 0) {
            ERR("failed to get socket name.");
            freeaddrinfo(res);
            return -1;
        }
        memcpy(&peer_addr, &(sa), addrlen);

        // start sender threads
        for(auto s: _vsocks) {
            // OUT("creating thread and fstat for sock %d", s);
            _sender_stats.insert(std::make_pair(s, fstat("OP_SEND")));
            // OUT("sock %d, stats: %s", s, string(_sender_stats.at(s)).c_str());
            std::thread *pth = new std::thread(&f_stream_interface::sender_thread, this, s);
            _sender_threads.push_back(pth);
            // create stats for each of the sender threads
        }
      
        freeaddrinfo(res);
        return 0;
    }

    virtual int accept() {
        return accept(lname, lport);
    }
    virtual int accept(char *_lname, char *_lport) {
        int err;
        int     buf, flags, ret;
        ssize_t sz;
        struct  sockaddr_storage sa_peer;

        // binding if the listen socket is not bound yet
        if(_sock_listen<0 && (err = bind(_lname, _lport)) < 0) {
            ERR("failed in bind.");
            return err;
        }

        /*
         * since UDP doesn't have a connection
         * the purpose of this function is just
         * to bind the client address to the sockets
         */
        if((err = listen()) < 0) {
            ERR("failed in listen.");
            return err;
        }

        for(auto s : _vsocks)
            sock_ops->close(s);
        _vsocks.clear();  // FIXME: senders are still working, can't just close data sockets
                          //        also the stats should be reset as well?
        
        if ((flags = rmfdflag(_sock_listen, O_NONBLOCK)) == -1) {
            ERR("failed to set nonblock socket.");
            return -1;
        }

        // creating sockets to recv data
        for(int i=0; i<_nsocks; i++) {
            int s;
            if((s = _accept(_lname, _lport))<0) {
                ERR("failed accept remote connection");
                return -1;
            }
            _vsocks.push_back(s);
            // safe_print("new connection %s:%d <=> %s:%d.",
            //     str_local_address(s).c_str(), local_port(s), str_peer_address(s).c_str(), peer_port(s));
        }
        initpollfds();

        // set the first socket address as peer address
        socklen_t addrlen = sizeof(struct sockaddr);
        // get the bound address info and set
        if (sock_ops->getpeername(_vsocks[0], (struct sockaddr *) &sa_peer, &addrlen) < 0) {
            ERR("failed to get socket name.");
            return -1;
        }
        memcpy(&peer_addr, &sa_peer, addrlen);

        // start sender threads
        for(auto s: _vsocks) {
            // OUT("creating thread and fstat for sock %d", s);
            _sender_stats.insert(std::make_pair(s, fstat("OP_SEND")));
            // OUT("sock %d, stats: %s", s, string(_sender_stats.at(s)).c_str());
            std::thread *pth = new std::thread(&f_stream_interface::sender_thread, this, s);
            _sender_threads.push_back(pth);
            // create stats for each of the sender threads
        }
        return _vsocks[0];
    }

    int bind() {
        return bind(lname, lport);
    }
    virtual int bind(char *_lname, char *_lport) {
        struct addrinfo hints, *res = nullptr;
        int opt, err;
        ASSUME(_lport, "local port must be specified before bind()");

        hints = _addrhints_for_bind();
        
        if((err = getaddrinfo(_lname,_lport,&hints,&res))<0) {
            ERR("failed to resolve local socket address.");
            return err;
        }

        // initialize the socket
        if ((_sock_listen = sock_ops->socket(res->ai_family, res->ai_socktype, 
                res->ai_protocol)) < 0)
        {
            ERR("failed to create socket.");
            return _sock_listen;
        }
        opt = 1;
        if ((err = sock_ops->setsockopt(_sock_listen, SOL_SOCKET, SO_REUSEPORT, 
                (char *) &opt, sizeof(opt))) < 0) 
        {
            sock_ops->close(_sock_listen); freeaddrinfo(res);
            ERR("failed to set socket options.");
            return err;
        }

        if ((err = sock_ops->bind(_sock_listen, (struct sockaddr *) res->ai_addr, 
                res->ai_addrlen)) < 0)
        {
            sock_ops->close(_sock_listen); freeaddrinfo(res);
            ERR("failed to bind socket.");
            return err;
        }

        struct sockaddr_storage sa;
        socklen_t len = sizeof(sa);
        if ((err = sock_ops->getsockname(_sock_listen, (struct sockaddr *)&sa, &len)) < 0) {
            sock_ops->close(_sock_listen); freeaddrinfo(res);
            ERR("failed to get socket name.");
            return err;
        }
        memcpy(&local_addr, &sa, len);

        freeaddrinfo(res);
        return 0;
    }

    void initpollfds() {
        int i = 0;
        for(auto s : _vsocks) {
            _pfd_in[i].events  = POLLIN;
            _pfd_in[i].fd      = s;
            _pfd_out[i].events = POLLOUT;
            _pfd_out[i++].fd   = s;
        }
    }
    virtual int enter_passive_mode() {
        int err;
        // OUT("enter_passive_mode: binding");
        if((err = bind()) < 0) {
            ERR("failed in bind.");
            return err;
        }

        // OUT("enter_passive_mode: listening");
        if((err = listen()) < 0) {
            ERR("failed in listen.");
            return err;
        }

        // OUT("enter_passive_mode: accepting");
        if((err = accept()) < 0) {
            ERR("failed in accept.");
            return err;
        }
        // OUT("enter_passive_mode: returning 0");
        return 0;
    }
    
    // public shared functions
    f_stream_interface(const char* local_hostname, const char* local_Port, const char* remote_hostname, const char* remote_port, int queue_type=QTMUTEX, int nsocks=1, bool mprtt=false) : _queue_type(queue_type), _nsocks(nsocks), _quit(0), _nsending(0), _roundtrip(mprtt), _sock_listen(-1) {
        lname = rname = lport = rport = nullptr;
        if(local_hostname) {
            lname = new char[strlen(local_hostname)+1];
            strcpy(lname, local_hostname);
        }
        if(local_Port) {
            lport = new char[strlen(local_Port)+1];
            strcpy(lport, local_Port);
        }
        if(remote_hostname) {
            rname = new char[strlen(remote_hostname)+1];
            strcpy(rname, remote_hostname);
        }
        if(remote_port) {
            rport = new char[strlen(remote_port)+1];
            strcpy(rport, remote_port);
        }
        _submit_stat.op_name = "OP_SUBMIT";
        _mprtt_stat .op_name = "OP_MPRTT";
        _sender_stat.op_name = "OP_SEND";
        _recver_stat.op_name = "OP_RECV";

        // creating sender queue based on the queue_type
        ASSUME(_queue_type==QTMUTEX||_queue_type==QTLOCKFREE, "unknown queue type.");
        if(_queue_type == QTMUTEX) {
            // safe_print("using mutex queue. socket number %d (on a single port)", _nsocks);
            send_q = new safe_queue<struct fbuf_t>(1000);
        }
        else {
            if(_nsocks != 1) {
                safe_print("\n***WARN***: using lockfree queue. socket number is limited to 1. the specified nsocks=%d is omitted.", _nsocks);
            }
            send_q  = new LockfreeQueue<struct fbuf_t>(100000);
            _nsocks = 1; // LockfreeQueue supports only one consumer
        }

        // std::cerr << "lname:" << (lname?lname:"null") << std::endl;
        // std::cerr << "lport:" << (lport?lport:"null") << std::endl;
        // std::cerr << "rname:" << (rname?rname:"null") << std::endl;
        // std::cerr << "rport:" << (rport?rport:"null") << std::endl;

        sock_ops = &socket_ops;  // default socket operations
    }

    virtual ~f_stream_interface() {
        // OUT("~f_stream_interface: enter.");
        _quit = true;
        if(send_q) {
            // OUT("~f_stream_interface: draining %d items in send_q.", send_q->size());
            sender_drain(1000);
        }
        // OUT("~f_stream_interface: deleting");
        delete lname, rname, lport, rport;
        // OUT("~f_stream_interface: joining");
        for(auto pth : _sender_threads) {
            pth->join();
        }
        // OUT("~f_stream_interface: joined");
        close();
        // OUT("~f_stream_interface: closed");
    }

    /**
     * @brief the sender thread function grabs buffers from the sender queue
     *        then send them over through the assigned socket
     * 
     * @param s the socket assigned to the thread
     */
    virtual void sender_thread(int s) {
        struct fbuf_t buf;
        thread_local static size_t nsend = 0;

        fstat & ss = _sender_stats.at(s);

        // safe_print("sender_thread %d started.", s);
        for(;;) {
            // OUT("sender_thread %d. send_q_size=%zu",s , send_q->size());
            try {
                buf = send_q->pop(100);
                _nsending++;
                nsend += buf.len;
                // safe_print("sender_thread %d: POP: buf.len=%d buf.buf='%.5s' qsize=%zu", s, buf.len, buf.buf, send_q->size());
            }
            catch(timeout_exception &e) {
                if(_quit) {
                    // OUT("sender_thread %d. timedout nsend=%zu", s, nsend);
                    break;
                }
                continue;
            }

            if(_send(s, buf.buf, buf.len, ss, buf.timeout, _roundtrip)<0) {
                ERR("sender_thread %d: failed sending packet. buf.len=%d buf.buf='%.5s' buf.timeout=%d", s, buf.len, buf.buf, buf.timeout);
            }
            // OUT("sender_thread %d: ss: %s", s, string(ss).c_str());
            _nsending--;
        }
    }

    /**
     * @brief send asynchronously
     *        push to sender queue then return success
     *        block if there's no place in queue
     * 
     * @param buf     the buffer to send
     * @param count   size of the data
     * @param timeout timeout in ms, 0 wait forever
     * @return int    number of bytes sent (either -1 or full size)
     */
    virtual int send_submit(char *buf, int count, int timeout=0) {
        auto  start = CounterClock::now();
        auto  dur   = CounterClock::now() - start;
        uint64_t nano=0, u64_test=0;
        
        thread_local struct fbuf_t fbuf;
        
        fbuf = {buf, count, timeout==0?-1:timeout};

        try {
            start = CounterClock::now();
            send_q->push(fbuf, timeout);
        }
        catch (timeout_exception &e) {
            errno = ETIMEDOUT;
            ERR("submit timeout!");
            return -1;
        }
        
        /* statistics of submit time */
        dur  = CounterClock::now() - start;
        nano = dur.count();
        if(nano>_submit_stat.max_lat_nano) {
            _submit_stat.max_lat_nano = nano;
        }
        else if(nano<_submit_stat.min_lat_nano) {
            _submit_stat.min_lat_nano = nano;
        }
        _submit_stat.tot_lat_nano += nano;
        _submit_stat.op_count++;

        return count;
    }

    virtual int close() {
        int ret;
        // close data sockets
        for (auto i : _vsocks)
        {
            ret = sock_ops->close(i);
            if(ret < 0)
                break;
        }
        sock_ops->close(_sock_listen);
        return ret;
    }

    virtual std::map<int, fstat> get_sender_stats() {
        return _sender_stats;
    }
    virtual fstat get_sender_stat() {
        for(auto pair : _sender_stats) {
            auto ss = pair.second;
            _sender_stat.avg_lat_nano += ss.avg_lat_nano;
            _sender_stat.max_lat_nano += ss.max_lat_nano;
            _sender_stat.min_lat_nano += ss.min_lat_nano;
            _sender_stat.nrecv        += ss.nrecv;
            _sender_stat.nsent        += ss.nsent;
            _sender_stat.op_count     += ss.op_count;
            _sender_stat.op_name       = ss.op_name;
        }
        return _sender_stat;
    }
    virtual fstat get_submit_stat() {
        return _submit_stat;
    }
    virtual fstat get_recver_stat() {
        return _recver_stat;
    }
    virtual void reset_submit_stat() {
        _submit_stat.max_lat_nano.store(0);
        _submit_stat.min_lat_nano.store(INT_MAX); 
        _submit_stat.avg_lat_nano.store(0);
        _submit_stat.tot_lat_nano.store(0);
        _submit_stat.op_count.store(0);
        _submit_stat.nsent.store(0);
        _submit_stat.nrecv.store(0);
    }
    virtual void reset_recver_stat() {
        _recver_stat.max_lat_nano.store(0);
        _recver_stat.min_lat_nano.store(INT_MAX); 
        _recver_stat.avg_lat_nano.store(0);
        _recver_stat.tot_lat_nano.store(0);
        _recver_stat.op_count.store(0);
        _recver_stat.nsent.store(0);
        _recver_stat.nrecv.store(0);
    }
    virtual void reset_sender_stats() {
        for(auto itr=_sender_stats.begin(); itr != _sender_stats.end(); itr++) {
            auto ss = (*itr).second;
            ss.max_lat_nano.store(0);
            ss.min_lat_nano.store(INT_MAX); 
            ss.avg_lat_nano.store(0);
            ss.tot_lat_nano.store(0);
            ss.op_count.store(0);
            ss.nsent.store(0);
            ss.nrecv.store(0);
        }
    }

    /**
     * @brief wait until the send_q empty and no outstanding sends
     *        Note: sender_drain takes at least 1ms to return
     *              so it shouldn't be used in primary IO path
     * 
     * @param timeout timeout in ms
     * @return int remaining element count after timeout or 0 if drained
     */
    virtual int sender_drain(int timeout=0) {
        auto start = std::chrono::steady_clock::now();
        auto dur   = MILLISECONDS(0);
        
        int ret = send_q->drain(timeout);
        if(ret!=0) {
            return ret;
        }

        /* 
         * sleep for 1 ms to allow the sender increase the out standing count
         * there could be a rare case that the last buffer in sender queue just got 
         * popped but the sender thread hasn't increased the _nsending yet
         * if checking _nsending in between of last buffer popping and the 
         * increasement of _nsending in sender_thread, the result could be wrong.
         */
        std::this_thread::sleep_for(MILLISECONDS(1));
        while(_nsending && (!timeout || dur < MILLISECONDS(timeout))) {
            // wait for outstanding sends to finish
            // OUT("sender_drain: outstanding %d", (uint8_t)_nsending);
            if(timeout)
                dur = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);
        }
        return _nsending;
    }

    virtual int send(char *buf, int count, int timeout=-1) {
        return _send(_vsocks[0], buf, count, _sender_stats.at(_vsocks[0]), timeout, _roundtrip);
    }
    virtual int _send(int _sock, char *buf, int count, fstat & ss_sender, int timeout=-1, bool roundtrip=false) {
        thread_local static std::chrono::high_resolution_clock::time_point  start;
        uint64_t nano=0;
        ushort   retry=10;
        int      nleft, ns, nr;

        // n_to_send += count;

        nleft = count;

        start = CounterClock::now();
        do {
            ns = sock_ops->fwriten(_sock, buf, nleft);
            // n_did_send += ns;
            if(ns<0) {
                // ERR("sock %d: failed to send. ns=%d nleft=%d peer_addr=%s:%d local_addr=%s:%d nerr=%d retry=%d", _sock, ns, nleft, str_peer_address(_sock).c_str(), peer_port(_sock), str_local_address(_sock).c_str(), local_port(_sock), nerr++, retry);
                break;
            }
            else if(ns==0) {
                if(waitout(_sock, timeout)==0) {
                    break;
                }
            }
            nleft -= ns;
            buf   += ns;
            if(nleft==0 && roundtrip) {
                // recving roundtrip packet
                if((nr = _recv(_sock, buf-count, count, timeout, false))<=0) {
                    if(errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK || errno == ETIMEDOUT) {
                        // ERR("roundtrip recv timedout timeout=%d retry=%d", timeout, retry);
                        buf  -= count;
                        nleft = count;
                        continue;
                    }
                    ns = -1;
                    ERR("_send: failed to receive for round trip ns=%d nr=%d retry=%d", ns, nr, retry);
                    break;
                }
            }
        } while(retry-- && nleft>0);

        /* statistics of send time */
        if(ns>0) {
            nano = (CounterClock::now() - start).count();
            if(nano>ss_sender.max_lat_nano) {
                ss_sender.max_lat_nano = nano;
            }
            else if(nano<ss_sender.min_lat_nano) {
                ss_sender.min_lat_nano = nano;
            }
            ss_sender.tot_lat_nano += nano;
            ss_sender.op_count++;
            ss_sender.nsent += ns;
            // OUT("_send: ns=%d ss_sender %s", ns, string(ss_sender).c_str());
        }
        return ns;
    }

    virtual int recv(char *buf, size_t count, int timeout=-1) {
        register int nr=0;
        TimePoint  start;
        thread_local static int next_idx;

        start = CounterClock::now();

        // if((nr=waitin(timeout))<0) {
        //     ERR("failed to recv. nr=%d", nr);
        //     return -1;
        // }
        // else if (nr==0) {
        //     ERR("timeout to recv.");
        //     errno = ETIMEDOUT;
        //     return -1;
        // }
        // int size = _vsocks.size();
        // for(int i=next_idx; i<size+next_idx; i++) {
        //     next_idx = i%size;
        //     if(_pfd_in[next_idx].revents & POLLIN) {
        //         // ready for read
        //         return nr=_recv(_pfd_in[next_idx].fd, buf, count, 0, _roundtrip, start);
        //     }
        // }

        // round robin all nonblock sockets
        int size = _vsocks.size(); int retry=1;
        do {
            for(int i=next_idx; i<size+next_idx; i++) {
                next_idx = i%size;
                if((nr=_recv(_pfd_in[next_idx].fd, buf, count, 0, _roundtrip, start))>0) {
                    return nr;
                }
            }

            if(retry && (nr=waitin(timeout))>0) {
                continue;
            }
            else if (nr==0) {
                // timedout
                break;
            }
        } while(retry--);

        return nr;
    }
    /**
     * @brief receive bytes from socket
     * 
     * @param buf      the buffer to receive data
     * @param count    the size of buffer
     * @param timeout  wait for data until timeout
     * @return int     bytes received, 
     *                 if return value <0 means error 
     *                 and the errno tells why
     */
    virtual int _recv(int s, char *buf, size_t count, int timeout, bool roundtrip, TimePoint starttime=TPEpoch) {
        uint64_t nano=0;
        int nr = 0, nw = 0, retry=1;

        if(starttime==TPEpoch)
            starttime = CounterClock::now();

        do {
            nr = sock_ops->freadn(s, buf, count);
            if(nr<0) {
                OUT("freadn(%d)=%d", s, nr);
                break;
            }
            else if(nr>0) {
                // n_did_recve += nr;
                break;
            }
            else if (errno==EAGAIN && timeout) {
              if(waitin(s, timeout)<=0) {
                  errno = ETIMEDOUT;
                  return -1;
              }
            }
        } while(retry--);
            
        /* statistics of recv time */
        if(nr>0) {
            if(roundtrip) {
                // for mprtt statistics, it's very common that
                // a client send a large packet to server
                // then server response with a small ack
                // so here i only want to send 1 bytes as response
                // with nagle's algorithm and delayed ack of TCP
                // this 1 byte ack could create real nasty latency
                if((nw = _send(s, buf, 1, _sender_stats.at(s), timeout, false))<0) {
                    ERR("recv: failed to send back ack. nw=%d", nw);
                }
            }
            nano = (CounterClock::now() - starttime).count();
            if(nano>_recver_stat.max_lat_nano) {
                _recver_stat.max_lat_nano = nano;
            }
            else if(nano<_recver_stat.min_lat_nano) {
                _recver_stat.min_lat_nano = nano;
            }
            _recver_stat.tot_lat_nano += nano;
            _recver_stat.nrecv        += nr;
            _recver_stat.op_count++;
        }
        // OUT("recv: stats %s", string(_recver_stat).c_str());
        if(nr<0)
            OUT("recv returning %d", nr);
        return nr;
    }

    virtual operator int() {
        return _vsocks[0];
    }
    virtual operator string () {
        return string_format("from %s:%d <= to => %s:%d",
            str_local_address().c_str(), local_port(), str_peer_address().c_str(), peer_port());
    }

    virtual int local_port () {
        return _port(&local_addr);
    }
    virtual string str_local_address () {
        return _str_addr(&local_addr);
    }
    virtual int peer_port () {
        return _port(&peer_addr);
    }
    virtual string str_peer_address () {
        return _str_addr(&peer_addr);
    }
    int local_port(int s) {
        struct sockaddr_storage sa;
        socklen_t addrlen = sizeof(struct sockaddr_storage);

        // get the bound address info and set
        if (sock_ops->getsockname(s, (struct sockaddr *) &sa, &addrlen) < 0) {
            ERR("failed to get socket name.");
            return -1;
        }
        return _port((struct sockaddr *)&sa);
    }
    string str_local_address(int s) {
        struct sockaddr_storage sa;
        socklen_t addrlen = sizeof(struct sockaddr_storage);

        // get the bound address info and set
        if (sock_ops->getsockname(s, (struct sockaddr *) &sa, &addrlen) < 0) {
            ERR("failed to get socket name.");
            return string("FAIL");
        }
        return _str_addr((struct sockaddr *)&sa);
    }
    int peer_port(int s) {
        struct sockaddr_storage sa;
        socklen_t addrlen = sizeof(struct sockaddr_storage);

        // set peer address since connect succeeded
        if (sock_ops->getpeername(s, (struct sockaddr *) &sa, &addrlen) < 0) {
            ERR("failed to get socket name.");
            return -1;
        }
        return _port((struct sockaddr *)&sa);
    }
    string str_peer_address(int s) {
        struct sockaddr_storage sa;
        socklen_t addrlen = sizeof(struct sockaddr_storage);

        // set peer address since connect succeeded
        if (sock_ops->getpeername(s, (struct sockaddr *) &sa, &addrlen) < 0) {
            ERR("failed to get socket name.");
            return string("FAIL");
        }
        return _str_addr((struct sockaddr *)&sa);
    }

    int _port (sockaddr *addr) {
        int  port;
        // detect address and convert to text
        if (((struct sockaddr *) addr)->sa_family == AF_INET) {
            // IPV4
            port = ntohs(((struct sockaddr_in *)addr)->sin_port);
        }
        else {
            // IPV6
            port = ntohs(((struct sockaddr_in6 *)addr)->sin6_port);
        }
        return port;
    }
    string _str_addr (struct sockaddr* addr) {
        char ip[INET6_ADDRSTRLEN];
        // detect peer address and convert to text
        if (addr->sa_family == AF_INET) {
            // IPV4
            inet_ntop(AF_INET, (void *) &((struct sockaddr_in *) addr)->sin_addr, ip, sizeof(ip));
        }
        else {
            // IPV6
            inet_ntop(AF_INET6, (void *) &((struct sockaddr_in6 *) addr)->sin6_addr, ip, sizeof(ip));
        }
        return string(ip);
    }


    /**
     * @brief set a socket flag
     * 
     * @param s     the file descriptor (socket) to unset
     * @param flag  the flag to set
     * @return int  the original socket flags
     */
    int addfdflag(int fd, int flag) {
        int flags = 0;
        flags = sock_ops->fcntl(fd, F_GETFL, 0);
        if (fcntl(fd, F_SETFL, flags | flag) == -1) {
            ERR("failed to set sock_ops->socket(%d) flag.", fd);
            return -1;
        }
        return flags;
    }
    /**
     * @brief unset a socket flag
     * 
     * @param s     the file descriptor (socket) to unset
     * @param flag  the flag to unset
     * @return int  the original socket flag
     */
    int rmfdflag(int fd, int flag) {
        int flags = 0;
        flags = sock_ops->fcntl(fd, F_GETFL, 0);
        if (fcntl(fd, F_SETFL, flags & ~flag) == -1) {
            ERR("failed to unset sock_ops->socket(%d) flag.", fd);
            return -1;
        }
        return flags;
    }
    
    /**
     * @brief wait until a tiven socket is ready for write or timeout
     * 
     * @param timeout timeout
     * @return 1 if ready, -1 if error, 0 if timeout
     */
    int waitout(int s, int timeout) {
        struct pollfd pfd;
        pfd.fd = s;
        pfd.events = POLLOUT;
        int ret, optval;
        socklen_t optlen;

        if ((ret = sock_ops->poll(&pfd, 1, timeout))<0) {
            ERR("waitout: poll error.");
            return -1;
        }
        if(!(pfd.revents & POLLOUT)) {
            // err on socket            
            optlen = sizeof(optval);
            if ((s, SOL_SOCKET, SO_ERROR, &optval, &optlen) == 0) {
                errno = optval;
                if(errno)
                    return -1;
            }
            else {
                return -1;
            }
        }
        return ret;
    }

    /**
     * @brief wait until the socket is ready for write or timeout
     * 
     * @param timeout timeout
     * @return int the first ready socket, -1 if error, 0 if timeout
     */
    int waitout(int timeout=-1) {
        int ret, size, optval;
        size = _vsocks.size();
        socklen_t optlen;

        if ((ret = sock_ops->poll(_pfd_out, size, timeout)) < 1) {
            if(ret<0)
                ERR("poll error. ret=%d", ret);
        }
        else {
            for(int i=0; i<size; i++) {
                if(_pfd_in[i].revents & POLLOUT) {
                    return _pfd_in[i].fd;
                }
            }
            // err on socket
            optlen = sizeof(optval);
            for(int i=0; i<size; i++) {
                if ((sock_ops->getsockopt(_pfd_in[i].fd, SOL_SOCKET, SO_ERROR,
                    &optval, &optlen)) == 0) {
                    errno = optval;
                    if(errno)
                        return -1;
                }
                else {
                    return -1;
                }
            }
        }
        return ret;
    }

    /**
     * @brief wait until a socket is ready for read or timeout
     * 
     * @param timeout timeout
     * @return int the first ready socket, -1 if error, 0 if timeout
     */
    int waitin(int timeout=-1) {
        int ret, size, optval;
        socklen_t optlen;

        size = _vsocks.size();
        if ((ret = sock_ops->poll(_pfd_in, size, timeout)) < 1) {
            if(ret<0)
                ERR("poll error. ret=%d", ret);
        }
        else {
            for(int i=0; i<size; i++) {
                if(_pfd_in[i].revents == POLLIN) {
                    return _pfd_in[i].fd;
                }
            }
            // err on socket
            optlen = sizeof(optval);
            for(int i=0; i<size; i++) {
                if ((sock_ops->getsockopt(_pfd_in[i].fd, SOL_SOCKET, SO_ERROR,
                    &optval, &optlen)) == 0) {
                    errno = optval;
                    if(errno)
                        return -1;
                }
                else {
                    return -1;
                }
            }
        }

        return ret;
    }
    /**
     * @brief wait until a tiven socket is ready for read or timeout
     * 
     * @param timeout timeout
     * @return 1 if ready, -1 if error, 0 if timeout
     */
    int waitin(int s, int timeout) {
        struct pollfd pfd;
        pfd.fd = s;
        pfd.events = POLLIN;
        int ret, optval;
        socklen_t optlen;

        if ((ret = sock_ops->poll(&pfd, 1, timeout))<0) {
            ERR("waitin: poll error.");
            return -1;
        }
        if(!(pfd.revents & POLLIN)) {
            // err on socket            
            optlen = sizeof(optval);
            if ((s, SOL_SOCKET, SO_ERROR, &optval, &optlen) == 0) {
                errno = optval;
                if(errno)
                    return -1;
            }
            else {
                return -1;
            }
        }
        return ret;
    }

    /**
     * @brief wait and expect a response from peer, 
     *        if the response string matches the 'expect_str'
     *        then send the 'send_str' to peer
     * 
     * @param expect_str the string to expect and compare with response
     * @param send_str   the string for sending when response matches
     * @param int        timeout, -1 wait forever
     * @return bool      true  if the response matches expect_str
     *                   false if the response doesn't match expect_str or timeout
     *                   if expect_str is null, any response is considered a match
     */
    bool strexpect(char* expect_str=nullptr, char* send_str=nullptr, int size=0, int timeout=-1) {
        thread_local int   buf_size = 0;
        thread_local static char *p = nullptr;
        int transfer_size;
        int n, retry=10;

        transfer_size = expect_str ? strlen(expect_str)+1 : 1;
        if(buf_size < transfer_size) {
            delete[] p;                   // delete[] itself will handle the nullptr
            p = new char[transfer_size];  // not deleting the largest buffer until the program termination
            buf_size = transfer_size;
        }
        if((n = recv(p, transfer_size, timeout))<0) {
            ERR("failed recv in expect. nrecv=%d", n);
            return false;
        }

        if(expect_str) {
            if(strcmp(expect_str, p)) {
                ERR("answer '%s' doesn't match '%s'", p, expect_str);
                return false;
            }
        }
        if(send_str) {
            if(!size) {
                size = strlen(send_str)+1;
            }
            if((n = send(send_str, size, timeout))<0) {
                ERR("failed sending %s in expect.", send_str);
                return false;
            }
        }

        return true;
    }

    /**
     * @brief send a buffer 'q' to peer then wait for it's response 'a'
     *        if q is null and q_size zeroed, qna just sends a minimal packet (1 byte) to peer
     *        if a is null and a_size zeroed, qna recvs a minimal packet (1 byte) from peer
     *        if q or a is null and the size isn't zero, the funtion generates a private buffer 
     *        to send or recv with the given sizes
     * 
     * @param q        the 'question'
     * @param q_size   size of q
     * @param a        the 'answer' from peer
     * @param a_size   size of a
     * @param timeout  timeout waiting for answer from peer
     * @return int     number of bytes of the answer, -1 if error and errno tells why
     */
    int qna(char* q=nullptr, size_t q_size=0, char* a=nullptr, size_t a_size=0, int timeout=-1) {
        thread_local size_t  buf_size = 0;
        thread_local static char *p   = new char[1]{'f'};
        thread_local static int nr, nw;

        thread_local static std::chrono::high_resolution_clock::time_point  start;
        thread_local static std::chrono::nanoseconds dur;
        uint64_t nano=0;
        if(!q) {
            if(!q_size) {
                q_size = 1;
            }
            else {
                if(q_size>buf_size) {
                    delete[] p;
                    p = new char[q_size]; // not deleting the largest buffer until the program termination
                    memset(p, 'f', q_size);
                    buf_size = q_size;
                }
            }
            q = p;
        }
        if(!a) {
            if(!a_size) {
                a_size = 1;
            }
            else {
                if(a_size>buf_size) {
                    delete[] p;
                    p = new char[a_size];
                    memset(p, 'f', q_size);
                    buf_size = a_size;
                }
            }
            a = p;
        }

        start = CounterClock::now();
        if((nw = send(q, q_size, timeout))<0) {
            ERR("failed sending in qna. nw=%d", nw);
            return nw;
        }

        if((nr = recv(a, a_size, timeout))<0) {
            ERR("failed recv in qna. n=%d", nr);
            return nr;
        }

        /* statistics of send time */
        if(nr>0 && nw>0) {
            nano = (CounterClock::now() - start).count();
            if(nano>_mprtt_stat.max_lat_nano) {
                _mprtt_stat.max_lat_nano = nano;
            }
            else if(nano<_mprtt_stat.min_lat_nano) {
                _mprtt_stat.min_lat_nano = nano;
            }
            _mprtt_stat.tot_lat_nano += nano;
            _mprtt_stat.nrecv        += nr;
            _mprtt_stat.nsent        += nw;
            _mprtt_stat.op_count++;
        }

        return true;
    }
};
class f_stream_udp : public f_stream_interface {
public:
    f_stream_udp(const char* local_hostname, const char* local_Port, const char* remote_hostname, const char* remote_port, int queue_type=QTMUTEX, int nsocks=1, bool roundtrip=false) : 
        f_stream_interface(local_hostname, local_Port, remote_hostname, remote_port, queue_type, nsocks, roundtrip) { prot = PROTUDP; }

    int _connect(char *_rname, char* _rport, int timeout=-1) override {
        struct addrinfo hints, *res = nullptr;
        struct sockaddr_storage sa;
        int    ret, on=1;
        socklen_t addrlen = sizeof(struct sockaddr_storage);

        hints = _addrhints_for_conn();
        
        if((ret = getaddrinfo(_rname, _rport, &hints, &res)) < 0) {
            ERR("failed to resolve remote socket address.");
            return ret;
        }

        /* 
         * for UDP, there's no concept of "connection"
         * connecting to a remote address on UDP sockets
         * only causes the target address bound to the socket
         * so that we can apply 'read/write' to the socket
         */
        int s;
        // initialize the socket
        if ((s = sock_ops->socket(res->ai_family, res->ai_socktype, res->ai_protocol)) < 0) {
            ERR("failed to create socket.");
            freeaddrinfo(res);
            return s;
        }
        if (sock_ops->setsockopt(s, SOL_SOCKET, SO_REUSEPORT, &on, sizeof(on)) < 0) {
            ERR("failed to set socket option.");
            sock_ops->close(s);
            freeaddrinfo(res);
            return -1;
        }

        int retry = 10; bool succ = false;
        do {
            // write something to peer listener address
            // let it know i am connecting
            int buf = 5678;    // this could be anything
            if(sendto(s, (char*)&buf, sizeof(buf), NULL, res->ai_addr, res->ai_addrlen)<0) {
                ERR("failed connecting to server.");
                continue;
            }

            // after knowning my address from its listener socket
            // the server would create a new data socket and send me something
            // read something from peer data socket
            // so i know the peer data address
            if(recvfrom(s, (char*)&buf, sizeof(buf), NULL, (sockaddr*)&sa, &addrlen)<0) {
                continue;
            }
            succ = true;
            break;
        } while(retry--);
        if(!succ) {
            freeaddrinfo(res);
            ERR("failed connecting to server.");
            return -1;
        }

        // bind peer address to socket
        if ((ret = sock_ops->connect(s, (sockaddr*)&sa, addrlen)) != 0 && errno == EINPROGRESS) {
            // for UDP this shouldn't happen
            if((ret = waitout(timeout)) < 0) {
                sock_ops->close(s);
                errno = ETIMEDOUT;
                freeaddrinfo(res);
                return ret;
            }
        }

        freeaddrinfo(res);
        return s;
    }

    int _accept(char *_lname, char *_lport) override {
        int     s, ret;
        ssize_t sz;
        struct sockaddr_storage sa;

        ASSUME(_sock_listen>0, "_sock_listen<=0");

        // initialize the socket
        if ((s = sock_ops->socket(local_addr.sa_family, SOCK_DGRAM, 0)) < 0) {
            ERR("failed to create socket.");
            return s;
        }
        int on = 1;
        if (sock_ops->setsockopt(s, SOL_SOCKET, SO_REUSEPORT, 
                (char *) &on, sizeof(on)) < 0) 
        {
            sock_ops->close(s);
            ERR("failed to set socket options.");
            return -1;
        }

        // receive a packet from the client
        // so we can tell the peer address of new connection
        socklen_t len = sizeof(sa);
        int buf=54321;
        if ((sz = sock_ops->recvfrom(_sock_listen, &buf, sizeof(buf), 0, (struct sockaddr *) &sa, &len)) < 0) {
            f_errno = FEACCEPT;
            return -1;
        }
        // bind peer address to socket
        if ((ret = sock_ops->connect(s, (struct sockaddr *)&sa, len)) != 0) {
            // for UDP this shouldn't happen
            ERR("failed to bind peer address for UDP");
            return -1;
        }

        // send something to peer to decide local address
        buf=54321;
        if(fwriten(s, (char*)&buf, sizeof(buf))<0) {
            sock_ops->close(s);
            ERR("failed binding UDP data socket.");
            return -1;
        }

        // make the socket non-block
        if ((addfdflag(s, O_NONBLOCK)) == -1) {
            sock_ops->close(s);
            ERR("failed to set nonblock socket.");
            return -1;
        }
        return s;
    }
    struct addrinfo _addrhints_for_bind() override {
        struct addrinfo hints;
        memset(&hints,0,sizeof(hints));
        hints.ai_family   = AF_UNSPEC;
        hints.ai_socktype = SOCK_DGRAM;
        hints.ai_protocol = 0;
        hints.ai_flags    = AI_PASSIVE|AI_ADDRCONFIG;
        return hints;
    }

    struct addrinfo _addrhints_for_conn() override {
        struct addrinfo hints;
        memset(&hints,0,sizeof(hints));
        hints.ai_family   = AF_UNSPEC;
        hints.ai_socktype = SOCK_DGRAM;
        hints.ai_protocol = 0;
        hints.ai_flags    = AI_ADDRCONFIG;
        return hints;
    }

    int listen() override {
        // UDP don't listen
        return 0;
    }
};
class f_stream_tcp : public f_stream_interface {
    bool                 _nodelay;
    bool                 _quickack;
public:
    f_stream_tcp(const char* local_hostname, const char* local_Port, const char* remote_hostname, const char* remote_port, int queue_type=QTMUTEX, int nsocks=1, bool roundtrip=false, bool nodelay=false, bool quickack=false) : 
        f_stream_interface(local_hostname, local_Port, remote_hostname, remote_port, queue_type, nsocks, roundtrip) {prot = PROTTCP;}

    int _connect(char *_rname, char* _rport, int timeout=-1) override {
        int       flags;
        int       ret, on=1;
        struct    addrinfo hints, *res = nullptr;
        struct    sockaddr_storage sa;
        socklen_t addrlen = sizeof(struct sockaddr_storage);

        hints = _addrhints_for_conn();

        if((ret = getaddrinfo(_rname, _rport, &hints, &res)) < 0) {
            ERR("failed to resolve remote socket address.");
            return ret;
        }

        int s;
        // initialize the multiple sockets
        if ((s = sock_ops->socket(res->ai_family, res->ai_socktype, res->ai_protocol)) < 0) {
            ERR("failed to create socket.");
            freeaddrinfo(res);
            return s;
        }

        if (sock_ops->setsockopt(s, SOL_SOCKET, SO_REUSEPORT, &on, sizeof(on)) < 0) {
            ERR("failed to set socket option.");
            sock_ops->close(s); freeaddrinfo(res);
            return -1;
        }

        if ((flags = rmfdflag(s, O_NONBLOCK)) == -1) {
            ERR("failed to set nonblock socket.");
            return -1;
        }

        // connect to peer and bind peer address to socket
        if ((sock_ops->connect(s, res->ai_addr, res->ai_addrlen)) < 0) {
            ERR("failed to connect remote server.");
            return -1;
        }

        if(_nodelay) {
            // OUT("_connect: set TCP_NODELAY");
            if (sock_ops->setsockopt(s, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on)) < 0) {
                sock_ops->close(s);
                ERR("failed set 'TCP_NODELAY'");
                return -1;
            }
        }
        if(_quickack) {
            // OUT("_connect: set TCP_QUICKACK");
            if (sock_ops->setsockopt(s, IPPROTO_TCP, TCP_QUICKACK, &on, sizeof(on)) < 0) {
                sock_ops->close(s);
                ERR("failed set 'TCP_QUICKACK'");
                return -1;
            }
        }

        /* FIXME: this is no longer applicable, just keep it for a note
        * unlike UDP, for TCP it can't reuse(bind) the same local address 
        * while connect to the same peer address at the same time
        * because it will create duplication 5-value tuples in kernel:
        * {<protocol>, <src addr>, <src port>, <dest addr>, <dest port>}
        * which uniquely identify a connection for TCP, and must be unique
        * for UDP, there's no connection either way, so UDP can have duplicated tuples
        */
      
        freeaddrinfo(res);
        return s;
    }

    int listen() override {
        return sock_ops->listen(_sock_listen, MAX_SOCKS);
    }

    int _accept(char *_lname, char *_lport) override {
        int    flags, err;
        struct sockaddr_storage sa_peer;
        socklen_t len = sizeof(sa_peer);
        int     on=1;

        // OUT("rm flag of _sock_listen(%d)", _sock_listen);
        if((flags = rmfdflag(_sock_listen, O_NONBLOCK)) == -1) {
            ERR("failed to set nonblock socket.");
            return -1;
        }

        int s;
        if((s = sock_ops->accept(_sock_listen, (sockaddr*)&sa_peer, &len))<0) {
            ERR("failed to accept connection.");
            return s;
        }
        if((flags = addfdflag(s, O_NONBLOCK)) == -1) {
            sock_ops->close(s);
            ERR("failed to set nonblock socket.");
            return -1;
        }
        if(_nodelay) {
            // OUT("accept: set TCP_NODELAY");
            if (sock_ops->setsockopt(s, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on)) < 0) {
                sock_ops->close(s);
                ERR("failed set 'TCP_NODELAY'");
                return -1;
            }
        }
        if(_quickack) {
            // OUT("accept: set TCP_QUICKACK");
            if (sock_ops->setsockopt(s, IPPROTO_TCP, TCP_QUICKACK, &on, sizeof(on)) < 0) {
                sock_ops->close(s);
                ERR("failed set 'TCP_QUICKACK'");
                return -1;
            }
        }
        return s;
    }
    struct addrinfo _addrhints_for_bind() override {
        struct addrinfo hints;
        memset(&hints,0,sizeof(hints));
        hints.ai_family   = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_protocol = 0;
        hints.ai_flags    = AI_PASSIVE|AI_ADDRCONFIG;
        return hints;
    }
    struct addrinfo _addrhints_for_conn() override {
        struct addrinfo hints;
        memset(&hints,0,sizeof(hints));
        hints.ai_family   = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_protocol = 0;
        hints.ai_flags    = AI_ADDRCONFIG;
        return hints;
    }

};

#ifdef AGM_RDMA
class f_stream_rdma_cm : public f_stream_interface {
    struct rdma_cm_id *listen_id = nullptr, *id = nullptr;
    char   *_recv_buf = nullptr, *_send_buf = nullptr;   // buffers for rdma send or receive
    size_t _recv_buf_size = 0  , _send_buf_size = 0;     // buffer sizes
    struct ibv_mr *_send_mr=nullptr, *_recv_mr=nullptr;
    struct ibv_pd *_pd = nullptr;
public:
    f_stream_rdma_cm(const char* local_hostname, const char* local_Port, const char* remote_hostname, const char* remote_port, int queue_type=QTMUTEX, int nsocks=1, bool roundtrip=false, size_t _recv_buf_size=0, size_t _send_buf_size=0) : 
        f_stream_interface(local_hostname, local_Port, remote_hostname, remote_port, queue_type, nsocks, roundtrip) 
    {
        prot = PROTRDMA;
        _sender_stats.insert(std::make_pair(0, fstat("OP_SEND")));
    }

    char* get_send_buffer(char **pbuf, int *size) {
        *pbuf = _send_buf;
        size  = size;
        return _send_buf;
    }
    char* get_recv_buffer(char **pbuf, int *size) {
        *pbuf = _recv_buf;
        size  = size;
        return _recv_buf;
    }
    char* set_send_buffer(char *buf, int size) {
        ASSUME(id, "rdma_cm_id hasn't been initialized yet.");
        // OUT("registering _send_buf: %p", buf);
        if(_send_buf != buf || _send_buf_size != size) {
            if(_send_mr) {
                rdma_dereg_mr(_send_mr);
            }
            // reallocate
            delete[] _send_buf;
            if(!buf)
                _send_buf = new char[size];
            else
                _send_buf = buf;
            _send_buf_size = size;
            _send_mr = rdma_reg_msgs(id, _send_buf, _send_buf_size);
            if (!_send_mr) {
                ERR("rdma_reg_msgs for send_buf");
                return nullptr;
            }
        }
        return _send_buf;
    }
    char* set_recv_buffer(char *buf, int size) {
        ASSUME(id, "rdma_cm_id hasn't been initialized yet.");
        // OUT("registering _recv_buf: %p", buf);
        if(_recv_buf != buf || _recv_buf_size != size) {
            if(_recv_mr) {
                rdma_dereg_mr(_recv_mr);
            }
            // reallocate
            delete[] _recv_buf;
            if(!buf)
                _recv_buf = new char[size];
            else
                _recv_buf = buf;
            _recv_buf_size = size;
            _recv_mr = rdma_reg_msgs(id, _recv_buf, _recv_buf_size);
            if (!_recv_mr) {
                ERR("rdma_reg_msgs for _recv_buf");
                return nullptr;
            }
        }
        return _recv_buf;
    }

    int connect(char *_rname, char* _rport, int timeout=-1) override {
        struct rdma_addrinfo hints, *res;
        struct ibv_qp_init_attr attr;
        int ret;

        memset(&hints, 0, sizeof hints);
        hints.ai_port_space = RDMA_PS_TCP;
        ret = rdma_getaddrinfo(_rname, _rport, &hints, &res);
        OUT("rdma_connect: %s:%s", _rname, _rport);
        if (ret) {
            ERR("rdma_getaddrinfo: %s\n", gai_strerror(ret));
            return ret;
        }

        memset(&attr, 0, sizeof attr);
        attr.cap.max_send_wr = attr.cap.max_recv_wr = 1;
        attr.cap.max_send_sge = attr.cap.max_recv_sge = 1;
        attr.cap.max_inline_data = 16;
        attr.qp_context = id;
        attr.sq_sig_all = 1;
        ret = rdma_create_ep(&id, res, NULL, &attr);
        if (ret) {
            ERR("rdma_create_ep");
            goto out_free_addrinfo;
        }

        ret = rdma_connect(id, NULL);
        if (ret) {
            perror("rdma_connect");
            goto out_free_addrinfo;
        }

        if ((addfdflag(id->channel->fd, O_NONBLOCK)) == -1) {
            ERR("failed to set nonblock socket.");
            goto out_free_addrinfo;
        }

        out_free_addrinfo:
            rdma_freeaddrinfo(res);
        out:
            return ret;
    }

    virtual int send(char *buf, int count, int timeout=-1) {
        return _send(id, buf, count, _sender_stats.at(0), timeout, _roundtrip);
    }
    virtual int _send(struct rdma_cm_id *_id, char *buf, int count, fstat & ss_sender, int timeout=-1, bool roundtrip=false) {
        thread_local static std::chrono::high_resolution_clock::time_point  start;
        uint64_t nano=0;
        int      nleft, ns, nr, ret;
    	struct ibv_wc wc;

        // n_to_send += count;

        start = CounterClock::now();

        ret = rdma_post_send(id, NULL, buf, count, _send_mr, 0);
        if (ret) {
            ERR("rdma_post_send");
            return -1;
        }

        while ((ret = rdma_get_send_comp(id, &wc)) == 0);
        if (ret < 0) {
            ERR("rdma_get_send_comp");
        }

        if (wc.status != IBV_WC_SUCCESS) {
            ERR("Failed status %s (%d) for wr_id %d\n", 
                ibv_wc_status_str(wc.status),
                wc.status, (int)wc.wr_id);
            return -1;
        }

        ns = wc.byte_len;

        if(roundtrip) {
            // recving roundtrip packet
            if((nr = _recv(_id, buf, count, timeout, false))<0) {
                ns = -1;
                ERR("_send: failed to receive for round trip ns=%d nr=%d", ns, nr);
            }
        }
        /* statistics of send time */
        if(ns>0) {
            nano = (CounterClock::now() - start).count();
            if(nano>ss_sender.max_lat_nano) {
                ss_sender.max_lat_nano = nano;
            }
            else if(nano<ss_sender.min_lat_nano) {
                ss_sender.min_lat_nano = nano;
            }
            ss_sender.tot_lat_nano += nano;
            ss_sender.op_count++;
            ss_sender.nsent += ns;
            // OUT("_send: ns=%d ss_sender %s", ns, string(ss_sender).c_str());
        }
        return ns;
    }

    int bind(char *_lname, char *_lport) {
    	struct rdma_addrinfo hints, *res;
    	struct ibv_qp_init_attr init_attr;
        int    ret;

        memset(&hints, 0, sizeof hints);
        hints.ai_flags = RAI_PASSIVE;
        hints.ai_port_space = RDMA_PS_TCP;
        hints.ai_qp_type = IBV_QPT_RC;

        // equal to socket ::getaddrinfo()
        ret = rdma_getaddrinfo(_lname, _lport, &hints, &res);
        if (ret) {
            ERR("rdma_getaddrinfo");
            return ret;
        }

        memset(&init_attr, 0, sizeof init_attr);
        init_attr.cap.max_send_wr = init_attr.cap.max_recv_wr = 1;
        init_attr.cap.max_send_sge = init_attr.cap.max_recv_sge = 1;
        init_attr.cap.max_inline_data = 16;
        init_attr.sq_sig_all = 1;
        // equal to socket ::socket()
        ret = rdma_create_ep(&listen_id, res, NULL, &init_attr);
        if (ret) {
            ERR("rdma_create_ep");
            goto out_free_addrinfo;
        }

        out_free_addrinfo:
            rdma_freeaddrinfo(res);
            return ret;
    }

    int listen() override {
        int ret;

        ASSUME(listen_id, "listen_id is not initialized.");
        
        // equal to socket sock_ops->listen()
        ret = rdma_listen(listen_id, 0);
        if (ret) {
            ERR("rdma_listen");
        }
        return ret;
    }

    int accept(char *_lname, char *_lport) override {
    	struct ibv_qp_init_attr init_attr;
        struct ibv_qp_attr qp_attr;
        struct ibv_wc wc;
        int ret;

        // binding if the listen socket is not bound yet
        if(!listen_id) {
            if((ret = bind(_lname, _lport)) < 0) {
                ERR("failed in bind.");
                return ret;
            }
            if((ret = listen())<0) {
                ERR("failed in listen");
                return ret;
            }
        }
        
        ASSUME(listen_id, "listen_id is null");

        ret = rdma_get_request(listen_id, &id);
        if (ret) {
            ERR("rdma_get_request");
            return ret;
        }

        // equal to socket sock_ops->accept()
        ret = rdma_accept(id, NULL);
        if (ret) {
            ERR("rdma_accept");
        }

        OUT("stream %s accepted.", _lport);

        if ((addfdflag(id->recv_cq_channel->fd, O_NONBLOCK)) == -1) {
            ERR("failed to set nonblock socket.");
            return -1;
        }

        return ret;
    }

    int recv(char *buf, size_t count, int timeout=-1) override {
        return _recv(id, buf, count, timeout, _roundtrip);
    }
    int _recv(struct rdma_cm_id *_id, char *buf, size_t count, int timeout, bool roundtrip, TimePoint starttime=TPEpoch) {
        uint64_t nano=0, timeout_nano = timeout * 1000;
        int nr = 0, nw = 0;
        int ret;
    	struct ibv_wc wc;
        rdma_cm_event *event;

        if(starttime==TPEpoch)
            starttime = CounterClock::now();

        ret = rdma_post_recv(_id, NULL, buf, count, _recv_mr);
        if (ret) {
            ERR("rdma_post_recv");
            return -1;
        }
        while ((ret = rdma_get_recv_comp(id, &wc)) <= 0) {
            if(timeout && ((CounterClock::now() - starttime).count()>timeout_nano)) {
                errno = EAGAIN;
                return 0;
            }
            if(errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK || errno == ETIMEDOUT) {
                // ERR("roundtrip recv timedout timeout=%d retry=%d", timeout, retry);
                continue;
            }
        }
        if (ret < 0) {
            ERR("rdma_get_recv_comp");
            return -1;
        }
        if (wc.status != IBV_WC_SUCCESS) {
            ERR("Failed status %s (%d) for wr_id %d\n", 
                ibv_wc_status_str(wc.status), wc.status, (int)wc.wr_id);
            return -1;
        }
            
        nr = wc.byte_len;
        // OUT("RDMA receives: %d bytes! (%.5s) timeout=%d", nr, buf, timeout);

        /* statistics of recv time */
        if(nr>0) {
            if(roundtrip) {
                if((nw = _send(_id, buf, 1, _sender_stats.at(0), timeout, false))<0) {
                    ERR("recv: failed to send back ack. nw=%d", nw);
                }
            }
            nano = (CounterClock::now() - starttime).count();
            if(nano>_recver_stat.max_lat_nano) {
                _recver_stat.max_lat_nano = nano;
            }
            else if(nano<_recver_stat.min_lat_nano) {
                _recver_stat.min_lat_nano = nano;
            }
            _recver_stat.tot_lat_nano += nano;
            _recver_stat.nrecv        += nr;
            _recver_stat.op_count++;
        }

        return nr;
    }

    ~f_stream_rdma_cm() {
        _quit = true;

        delete lname, rname, lport, rport;
        for(auto pth : _sender_threads) {
            pth->join();
        }
        if(id) {
            rdma_disconnect(id);
            rdma_destroy_ep(id);
        }
        if(_send_mr)
            rdma_dereg_mr(_send_mr);
        if(_recv_mr)
            rdma_dereg_mr(_recv_mr);
    	if(listen_id)
            rdma_destroy_ep(listen_id);
        if(_pd)
            ibv_dealloc_pd(_pd);
        delete[] _recv_buf,_send_buf;
    }

    int send_submit(char *buf, int count, int timeout=0) final {
        auto  start = CounterClock::now();
        auto  dur   = CounterClock::now() - start;
        uint64_t nano;

        start = CounterClock::now();
        nano = (CounterClock::now() - start).count();

        if(nano>_submit_stat.max_lat_nano) {
            _submit_stat.max_lat_nano = nano;
        }
        else if(nano<_submit_stat.min_lat_nano) {
            _submit_stat.min_lat_nano = nano;
        }
        _submit_stat.tot_lat_nano += nano;
        _submit_stat.op_count++;

        return send(buf, count, timeout);
    }
    int sender_drain(int timeout=0) final {
        return 0;
    }

    operator int() final {
        return 0;
    }

    struct addrinfo _addrhints_for_bind() final {}
    struct addrinfo _addrhints_for_conn() final {}
    int _accept(char *_rname, char* _rport) final {}
    int _connect(char *_rname, char* _rport, int timeout=-1) final {}
};

class f_stream_rdma_rsocket : public f_stream_interface {
    char   *_recv_buf = nullptr, *_send_buf = nullptr;   // buffers for rdma send or receive
    size_t _recv_buf_size = 0  , _send_buf_size = 0;     // buffer sizes
public:
    f_stream_rdma_rsocket(const char* local_hostname, const char* local_Port, const char* remote_hostname, const char* remote_port, int queue_type=QTMUTEX, int nsocks=1, bool roundtrip=false) : 
        f_stream_interface(local_hostname, local_Port, remote_hostname, remote_port, queue_type, nsocks, roundtrip) 
    {
        sock_ops = &rsocket_ops;
        prot = PROTTCP;
    }

    int _connect(char *_rname, char* _rport, int timeout=-1) override {
        int       flags;
        int       ret, on=1;
    	struct    rdma_addrinfo hints, *res = nullptr;
        struct    sockaddr_storage sa;
        socklen_t addrlen = sizeof(struct sockaddr_storage);

        hints.ai_flags |= RAI_PASSIVE;
        hints.ai_qp_type = IBV_QPT_RC;
        hints.ai_port_space = RDMA_PS_TCP;

        if((ret = rdma_getaddrinfo(_rname, _rport, &hints, &res)) < 0) {
            ERR("failed to resolve remote socket address.");
            return ret;
        }

        int s;
        // initialize the multiple sockets
        if ((s = sock_ops->socket(res->ai_family, SOCK_STREAM, 0)) < 0) {
            ERR("failed to create socket.");
            rdma_freeaddrinfo(res);
            return s;
        }

        // if (sock_ops->setsockopt(s, SOL_SOCKET, SO_REUSEPORT, &on, sizeof(on)) < 0) {
        //     ERR("failed to set socket option.");
        //     sock_ops->close(s); rdma_freeaddrinfo(res);
        //     return -1;
        // }

        if ((flags = rmfdflag(s, O_NONBLOCK)) == -1) {
            ERR("failed to set nonblock socket.");
            sock_ops->close(s); rdma_freeaddrinfo(res);
            return -1;
        }

        // if(sock_ops->bind(s, res->ai_src_addr, res->ai_src_len) != 0) {
        //     ERR("rbind");
        //     sock_ops->close(s); rdma_freeaddrinfo(res);
        //     return -1;
        // }

        if(res->ai_route) {
            ret = sock_ops->setsockopt(s, SOL_RDMA, RDMA_ROUTE, res->ai_route,
                        res->ai_route_len);
            if (ret) {
                ERR("rsetsockopt RDMA_ROUTE");
                sock_ops->close(s); rdma_freeaddrinfo(res);
                return -1;
            }
        }

        // connect to peer and bind peer address to socket
        if (sock_ops->connect(s, res->ai_dst_addr, res->ai_dst_len) < 0) {
            ERR("failed to connect remote server.");
            sock_ops->close(s); rdma_freeaddrinfo(res);
            return -1;
        }

        rdma_freeaddrinfo(res);
        return s;
    }

    int listen() override {
        return sock_ops->listen(_sock_listen, MAX_SOCKS);
    }

    int _accept(char *_lname, char *_lport) override {
        int    flags, err;
        struct sockaddr_storage sa_peer;
        socklen_t len = sizeof(sa_peer);
        int     on=1;

        // OUT("rm flag of _sock_listen(%d)", _sock_listen);
        if((flags = rmfdflag(_sock_listen, O_NONBLOCK)) == -1) {
            ERR("failed to set nonblock socket.");
            return -1;
        }

        int s;
        if((s = sock_ops->accept(_sock_listen, (sockaddr*)&sa_peer, &len))<0) {
            ERR("failed to accept connection.");
            return s;
        }
        if((flags = addfdflag(s, O_NONBLOCK)) == -1) {
            sock_ops->close(s);
            ERR("failed to set nonblock socket.");
            return -1;
        }
        return s;
    }

    char* get_send_buffer(char **pbuf, int *size) {
        *pbuf = _send_buf;
        size  = size;
        return _send_buf;
    }
    char* get_recv_buffer(char **pbuf, int *size) {
        *pbuf = _recv_buf;
        size  = size;
        return _recv_buf;
    }
    char* set_send_buffer(char *buf, int size) {
        int ret, i, t;
        off_t offset;

        if(_send_buf != buf || _send_buf_size != size) {
            // reallocate
            delete[] _send_buf;
            if(!buf)
                _send_buf = new char[size];
            else
                _send_buf = buf;
            _send_buf_size = size;
            if( riomap(_vsocks[0], buf, size, PROT_WRITE, 0, 0) < 0) {
                ERR("riomap");
                return nullptr;
            }
        }
        return _send_buf;
    }
    char* set_recv_buffer(char *buf, int size) {
        int ret, i, t;
        off_t offset;

        if(_recv_buf != buf || _recv_buf_size != size) {
            // reallocate
            delete[] _recv_buf;
            if(!buf)
                _recv_buf = new char[size];
            else
                _recv_buf = buf;
            _recv_buf_size = size;
            if( riomap(_vsocks[0], buf, size, PROT_WRITE, 0, 0) < 0) {
                ERR("riomap");
                return nullptr;
            }
        }
        return _recv_buf;
    }

    int bind() {
        return bind(lname, lport);
    }
    virtual int bind(char *_lname, char *_lport) {
        struct rdma_addrinfo hints, *res = nullptr;
        int opt, err;
        ASSUME(_lport, "local port must be specified before bind()");

        hints.ai_flags |= RAI_PASSIVE;
        hints.ai_qp_type = IBV_QPT_RC;
        hints.ai_port_space = RDMA_PS_TCP;
        
        if((err = rdma_getaddrinfo(_lname,_lport,&hints,&res))<0) {
            ERR("failed to resolve local socket address. %s:%s", _lname, _lport);
            return err;
        }

        // initialize the socket
        if ((_sock_listen = sock_ops->socket(res->ai_family, SOCK_STREAM, 0)) < 0)
        {
            ERR("failed to create socket.");
            return _sock_listen;
        }

        // opt = 1;
        // if ((err = sock_ops->setsockopt(_sock_listen, SOL_SOCKET, SO_REUSEPORT, 
        //         (char *) &opt, sizeof(opt))) < 0) 
        // {
        //     sock_ops->close(_sock_listen); rdma_freeaddrinfo(res);
        //     ERR("failed to set socket options.");
        //     return err;
        // }

        if ((err = sock_ops->bind(_sock_listen, (struct sockaddr *) res->ai_src_addr, res->ai_src_len)) < 0)
        {
            sock_ops->close(_sock_listen); rdma_freeaddrinfo(res);
            ERR("failed to bind socket.");
            return err;
        }

        struct sockaddr_storage sa;
        socklen_t len = sizeof(sa);
        if ((err = sock_ops->getsockname(_sock_listen, (struct sockaddr *)&sa, &len)) < 0) {
            sock_ops->close(_sock_listen); rdma_freeaddrinfo(res);
            ERR("failed to get socket name.");
            return err;
        }
        memcpy(&local_addr, &sa, len);

        rdma_freeaddrinfo(res);
        return 0;
    }
    struct addrinfo _addrhints_for_bind() final {}
    struct addrinfo _addrhints_for_conn() final {}
};


class f_stream_rdma_ibverbs : public f_stream_interface {
    char   *_recv_buf     = nullptr, *_send_buf = nullptr;   // buffers for rdma send or receive
    size_t _recv_buf_size = 0, _send_buf_size = 0;     // buffer sizes
    bool   _send_buf_selfmanage = false, _recv_buf_selfmanage = false;
    int    _num_devices;
    f_stream_interface *_stream = nullptr;
    bool               _stream_selfmanage = false;
    struct ibv_context  *_dev_context = nullptr;
    struct ibv_pd       *_protection_domain = nullptr;
    struct ibv_cq       *_sender_completion_queue, *_recver_completion_queue;
    struct ibv_mr       *_send_mr=nullptr, *_recv_mr=nullptr;
    struct ibv_device  **_device_list;
    struct ibv_qp       *_queue_pair;
    uint32_t _peer_qp_num, _local_qp_num;
    uint16_t _peer_local_id, _local_id;
    union  ibv_gid _peer_gid, _local_gid;
    struct ibv_sge *_send_sge=nullptr, *_recv_sge=nullptr;
    char   _rdmadev[20];
    int    _cqe;
    std::atomic_int_fast16_t _outstanding_sends, _outstanding_recvs;
    std::vector<std::thread*> _pth_pollers;
public:
    int alloc_pd() {
        // create protection domain
        if((_protection_domain = ibv_alloc_pd(_dev_context)) == nullptr) {
            ERR("failed creating pd.");
            return -1;
        }
        return 0;
    }
    int create_cq() {
        // create completion queue
        if ((_sender_completion_queue  = ibv_create_cq(_dev_context, _cqe, nullptr, nullptr, 0)) == nullptr) {
            ERR("failed creating sender cq.");
            return -1;
        }
        if ((_recver_completion_queue  = ibv_create_cq(_dev_context, _cqe, nullptr, nullptr, 0)) == nullptr) {
            ERR("failed creating sender cq.");
            return -1;
        }
        _pth_pollers.push_back(new std::thread(&f_stream_rdma_ibverbs::sender_poller, this));
        _pth_pollers.push_back(new std::thread(&f_stream_rdma_ibverbs::recver_poller, this));
        return 0;
    }
    int create_qp() {
        // create queue pair
        struct ibv_qp_init_attr queue_pair_init_attr;

        memset(&queue_pair_init_attr, 0, sizeof(queue_pair_init_attr));
        queue_pair_init_attr.qp_type = IBV_QPT_RC;
        queue_pair_init_attr.sq_sig_all = 1;       // if not set 0, all work requests submitted to SQ will always generate a Work Completion.
        queue_pair_init_attr.send_cq = _sender_completion_queue;         // completion queue can be shared or you can use distinct completion queues.
        queue_pair_init_attr.recv_cq = _recver_completion_queue;         // completion queue can be shared or you can use distinct completion queues.
        queue_pair_init_attr.cap.max_send_wr = _cqe;  // increase if you want to keep more send work requests in the SQ.
        queue_pair_init_attr.cap.max_recv_wr = _cqe;  // increase if you want to keep more receive work requests in the RQ.
        queue_pair_init_attr.cap.max_send_sge = 1; // increase if you allow send work requests to have multiple scatter gather entry (SGE).
        queue_pair_init_attr.cap.max_recv_sge = 1; // increase if you allow receive work requests to have multiple scatter gather entry (SGE).

        if((_queue_pair = ibv_create_qp(_protection_domain, &queue_pair_init_attr)) == nullptr) {
            ERR("failed to create qp. _cqe=%d", _cqe);
            return -1;
        }

        return 0;
    }

    int init_qp() {
        // change queue pair state to 'init'
        struct ibv_qp_attr init_attr;
        memset(&init_attr, 0, sizeof(init_attr));
        init_attr.qp_state = ibv_qp_state::IBV_QPS_INIT;
        init_attr.port_num = 1;
        init_attr.pkey_index = 0;
        init_attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;

        return ibv_modify_qp(_queue_pair, &init_attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS) == 0 ? 0 : -1;
    }

    int exchange_conn_info(int mode) {
        char     q1[] = "qp_num";
        char     q2[] = "lid";
        char     q3[] = "gid";

        union    ibv_gid local_gid;
        ibv_port_attr port_attr;
        ibv_query_port(_dev_context, 1, &port_attr);

		if (ibv_query_gid(_dev_context, 1, 0, &local_gid)) {
			ERR("can't read sgid of index %d\n", 0);
			return -1;
		}
        _local_qp_num = _queue_pair->qp_num;
        _local_gid    = local_gid;
        _local_id     = port_attr.lid;
        // OUT("_queue_pair->qp_num=%zu", _queue_pair->qp_num);
        // OUT("port_attr.lid=%zu", port_attr.lid);
        // OUT("local_gid=%zu", local_gid);

        if(mode == 0) {
            // passive
            if(!_stream) {
                _stream = new f_stream_tcp(lname, lport, rname, rport);
                _stream_selfmanage = true;
                _stream->accept();
            }
            if(_stream->qna(q1, sizeof(q1), (char*)&_peer_qp_num, sizeof(uint32_t))<0) {
                ERR("failed to exchange destination_qp_number");
                return -1;
            }
            if(_stream->qna(q2, sizeof(q2), (char*)&_peer_local_id, sizeof(uint16_t))<0) {
                ERR("failed to exchange destination_local_id");
                return -1;
            }
            if(_stream->qna(q3, sizeof(q3), (char*)&_peer_gid, sizeof(union ibv_gid))<0) {
                ERR("failed to exchange destination_local_id");
                return -1;
            }
            _stream->strexpect(q1, (char*)&(_queue_pair->qp_num), sizeof(_queue_pair->qp_num));
            _stream->strexpect(q2, (char*)&(port_attr.lid), sizeof(port_attr.lid));
            _stream->strexpect(q3, (char*)&(local_gid), sizeof(union ibv_gid));
        }
        else {
            // active
            if(!_stream) {
                _stream = new f_stream_tcp(lname, lport, rname, rport);
                _stream_selfmanage = true;
                _stream->connect();
            }
            _stream->strexpect(q1, (char*)&(_queue_pair->qp_num), sizeof(_queue_pair->qp_num));
            _stream->strexpect(q2, (char*)&(port_attr.lid), sizeof(port_attr.lid));
            _stream->strexpect(q3, (char*)&(local_gid), sizeof(union ibv_gid));
            if(_stream->qna(q1, sizeof(q1), (char*)&_peer_qp_num, sizeof(uint32_t))<0) {
                ERR("failed to exchange destination_qp_number");
                return -1;
            }
            if(_stream->qna(q2, sizeof(q2), (char*)&_peer_local_id, sizeof(uint16_t))<0) {
                ERR("failed to exchange destination_local_id");
                return -1;
            }
            if(_stream->qna(q3, sizeof(q3), (char*)&_peer_gid, sizeof(union ibv_gid))<0) {
                ERR("failed to exchange destination_local_id");
                return -1;
            }
        }
        // OUT("peer_qp_num=%zu", *peer_qp_num);
        // OUT("peer_local_id=%zu", *peer_local_id);
        // OUT("peer_gid=%zu", *peer_gid);
        return 0;
    }

    int connect_peer() {
        // change queue pair state to 'ready to receive'
        struct ibv_qp_attr rtr_attr;

        // OUT("_peer_qp_num=%zu _peer_local_id=%zu _peer_gid.global.interface_id=%zu _peer_gid.global.subnet_prefix=%zu", 
        //     _peer_qp_num, _peer_local_id, _peer_gid.global.interface_id,_peer_gid.global.subnet_prefix);

        memset(&rtr_attr, 0, sizeof(rtr_attr));
        rtr_attr.qp_state               = ibv_qp_state::IBV_QPS_RTR;
        rtr_attr.path_mtu               = ibv_mtu::IBV_MTU_1024;
        rtr_attr.rq_psn                 = 0;
        rtr_attr.max_dest_rd_atomic     = _cqe;
        rtr_attr.min_rnr_timer          = 1;
        rtr_attr.ah_attr.sl             = 0;
        rtr_attr.ah_attr.src_path_bits  = 0;
        rtr_attr.ah_attr.port_num       = 1;
        rtr_attr.ah_attr.is_global      = 1;
        rtr_attr.ah_attr.grh.hop_limit  = 1;
        rtr_attr.ah_attr.grh.dgid       = _peer_gid;
        rtr_attr.ah_attr.grh.sgid_index = 0;
        rtr_attr.ah_attr.dlid           = _peer_local_id;
        rtr_attr.dest_qp_num            = _peer_qp_num;

        if(ibv_modify_qp(_queue_pair, &rtr_attr, IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER) != 0) {
            ERR("failed to set qp state to RTR");
            return -1;
        }

        // change queue pair state to 'ready to send'
        struct ibv_qp_attr rts_attr;
        memset(&rts_attr, 0, sizeof(rts_attr));
        rts_attr.qp_state = ibv_qp_state::IBV_QPS_RTS;
        rts_attr.timeout = 1;
        rts_attr.retry_cnt = 7;
        rts_attr.rnr_retry = 7;
        rts_attr.sq_psn = 0;
        rts_attr.max_rd_atomic = _cqe;

        if(ibv_modify_qp(_queue_pair, &rts_attr, IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC) != 0) {
            ERR("failed to set queue pair state to 'ready to send'");
            return -1;
        }
        return 0;
    }

    string str_peer_address() override {
        char ip[64];
        gid_to_str(&_peer_gid, ip);
        return string(ip);
    }
    int peer_port() override {
        return _peer_qp_num;
    }
    string str_local_address() override {
        char ip[64];
        gid_to_str(&_local_gid, ip);
        return string(ip);
    }
    int local_port() override {
        return _local_qp_num;
    }

    int post_send(size_t size) {
        thread_local static struct ibv_send_wr wr={0}, *bad = nullptr;

        while(_outstanding_sends>=_cqe) {}

        _send_sge->length = size;
        wr.sg_list = _send_sge;
        wr.num_sge = 1;
        wr.wr_id = std::chrono::high_resolution_clock::now().time_since_epoch().count();

        // All WRs that are posted into Send Queue (SQ) are posted via ibv_send_wr.
        // You should specify the opcode so that which operation you want to do.
        wr.opcode = IBV_WR_SEND;
        // With IBV_SEND_SIGNALED flag, the hardware creates a work completion (wc) entry into the completion queue connected to the send queue.
        // You can wait with ibv_poll_cq() call until it finishes its operation.
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.next = nullptr;

        if(ibv_post_send(_queue_pair, &wr, &bad)<0) {
            ERR("failed to post send request.");
            return -1;
        }
        
        _outstanding_sends++;

        return 0;
    }

    int post_recv(size_t size, bool _outstanding=true) {
        struct ibv_recv_wr wr, *bad = nullptr;

        if(!_recv_sge) {
            return -1;
        }

        while(_outstanding_recvs >= _cqe){
            if(_quit) break;
        }
        
        _recv_sge->length = size;
        wr.sg_list = _recv_sge;
        wr.num_sge = 1;
        // will be used for identification.
        // When a request fail, ibv_poll_cq() returns a work completion (struct ibv_wc) with the specified wr_id.
        // If the wr_id is 100, we can easily find out that this RECV request failed.
        wr.wr_id = std::chrono::high_resolution_clock::now().time_since_epoch().count();
        // You can chain several receive requests to reduce software footprint, hnece to improve latency.
        wr.next = nullptr;

        // If posting fails, the address of the failed WR among the chained WRs is stored in bad_wr.
        if(ibv_post_recv(_queue_pair, &wr, &bad)<0) {
            ERR("failed to post recv request.");
            return -1;
        }

        if(_outstanding)
            _outstanding_recvs++;

        return 0;
    }

    /**
     * @brief poll for completion
     * 
     * @param timeout time in ms to allow polling, waitforever if -1, 0 nonblock
     * @return int    number of work completions if success
     *                -1 error
     *                0  no entry in cq after timeout, errno is set to ETIMEDOUT
     */
    int poll_recver_completion(int timeout, int nwr=1) {
        struct ibv_wc wc[10] = {{0}};
        int result = 0;

        using clock = std::chrono::steady_clock;
        using ms    = std::chrono::milliseconds;
        auto start = clock::now();
        do {
            // ibv_poll_cq returns the number of WCs that are newly completed,
            // If it is 0, it means no new work completion is received.
            // Here, the second argument specifies how many WCs the poll should check,
            if(timeout != -1 && std::chrono::duration_cast<ms>(clock::now()-start).count()>=timeout) {
                // OUT("poll_completion： returning %d", timeout);
                errno = ETIMEDOUT;
                return 0;
            }
            result += ibv_poll_cq(_recver_completion_queue, 10, wc);
            // if(result>0)
            //     OUT("poll_recver_completion: result=%d timeout=%d", result, timeout);
        } while (result < nwr && timeout);

        _outstanding_recvs -= result;

        for(int i=0; i<result; i++) {
            if (wc[i].status != ibv_wc_status::IBV_WC_SUCCESS) {
                // success
                // OUT("poll_completion： returning 0");
                return -1;
            }
        }

        // You can identify which WR failed with wc.wr_id.
        // ERR("Poll failed with status %s (work request ID: %llu)\n", ibv_wc_status_str(wc.status), wc.wr_id);
        if(result == 0)
            errno = ETIMEDOUT;
        return result;
    }
    int poll_sender_completion(int timeout, int nwr=1) {
        struct ibv_wc wc[10] = {{0}};
        int result = 0;

        // OUT("poll_completion: timeout=%d", timeout);

        using clock = std::chrono::steady_clock;
        using ms    = std::chrono::milliseconds;
        auto start = clock::now();
        do {
            // ibv_poll_cq returns the number of WCs that are newly completed,
            // If it is 0, it means no new work completion is received.
            // Here, the second argument specifies how many WCs the poll should check,
            if(timeout != -1 && std::chrono::duration_cast<ms>(clock::now()-start).count()>=timeout) {
                // OUT("poll_completion： returning %d", timeout);
                errno = ETIMEDOUT;
                return 0;
            }
            result += ibv_poll_cq(_sender_completion_queue, 10, wc);
            // if(result>0)
            //     OUT("poll_sender_completion: result=%d timeout=%d", result, timeout);
        } while (result < nwr && timeout);
        
        _outstanding_sends -= result;
        
        for(int i=0; i<result; i++) {
            if (wc[i].status != ibv_wc_status::IBV_WC_SUCCESS) {
                // success
                // OUT("poll_completion： returning 0");
                return -1;
            }
        }

        // You can identify which WR failed with wc.wr_id.
        // ERR("Poll failed with status %s (work request ID: %llu)\n", ibv_wc_status_str(wc.status), wc.wr_id);
        if(result == 0)
            errno = ETIMEDOUT;
        return result;
    }

    int accept(char *_lname, char *_lport) override {
        if(alloc_pd()<0) {
            return -1;
        }
        if(create_cq()<0) {
            return -1;
        }
        if(create_qp()<0) {
            return -1;
        }
        if(init_qp()<0) {
            return -1;
        }
  
        // exchange queue pair number and global id with peer
        if(exchange_conn_info(0)<0) {
            return -1;
        }

        // connect to peer
        if(connect_peer()<0) {
            return -1;
        }

        // OUT("server connected.");
        return 0;
    }
    int connect(char *_rname, char* _rport, int timeout=-1) override {
        if(alloc_pd()<0) {
            return -1;
        }
        if(create_cq()<0) {
            return -1;
        }
        if(create_qp()<0) {
            return -1;
        }
        if(init_qp()<0) {
            return -1;
        }
  
        // exchange queue pair number and global id with peer
        if(exchange_conn_info(1)<0) {
            return -1;
        }

        // connect to peer
        if(connect_peer()<0) {
            return -1;
        }
        // OUT("client connected.");
        return 0;
    }

    void gid_to_str(const union ibv_gid *gid, char wgid[])
    {
        uint32_t tmp_gid[4];
        int i;

        memcpy(tmp_gid, gid, sizeof(tmp_gid));
        for (i = 0; i < 4; ++i)
            sprintf(&wgid[i * 8], "%08x", htobe32(tmp_gid[i]));
    }

    f_stream_rdma_ibverbs(const char* local_hostname, const char* local_Port, const char* remote_hostname, const char* remote_port, int queue_type=QTMUTEX, int nsocks=1, bool roundtrip=false, size_t _recv_buf_size=0, size_t _send_buf_size=0, f_stream_interface *stream = nullptr, char *rdmadev = nullptr) : 
        f_stream_interface(local_hostname, local_Port, remote_hostname, remote_port, queue_type, nsocks, roundtrip) 
    {
        prot     = PROTRDMA;
        _stream  = stream;
        if(rdmadev)
            strcpy(_rdmadev, rdmadev);
        _outstanding_sends = 0;
        _outstanding_recvs = 0;
        _sender_stats.insert(std::make_pair(0, fstat("OP_SEND")));

        // open rdma device
        _device_list = ibv_get_device_list( &_num_devices );
        ASSUME(_num_devices>0, "no RDMA device found!!!");

        for(int i=0; i<_num_devices; i++) {
            ibv_gid     gid;
            char        ip[64];
            ibv_context *ctx;
            char        devname[20];
            strcpy(devname, ibv_get_device_name(_device_list[i]));
            if((strlen(_rdmadev) != 0 && !strcmp(devname, _rdmadev)) ||
               (strlen(_rdmadev) == 0 && i == 0)) 
            {
                _dev_context = ibv_open_device(_device_list[i]);
                ibv_query_gid(_dev_context, 1, 0, &gid);
                gid_to_str(&gid, ip);
                strcpy(_rdmadev, devname);
                // OUT("open %s gid: %s", devname, ip);
            }
        }
        ASSUME(_dev_context, "failed to open device.");

        struct ibv_device_attr dev_attr;
        if(ibv_query_device(_dev_context, &dev_attr)<0) {
            ERR("failed to get device attr.");
            _cqe = 10;
        }
        // _cqe = dev_attr.max_cqe;  // not working
        _cqe = 10;
    }
    ~f_stream_rdma_ibverbs() {
        _quit = true;
        for(auto pth : _pth_pollers) {
            pth->join();
        }
        if(_stream && _stream_selfmanage)
            delete _stream;
        if(_send_mr)
            rdma_dereg_mr(_send_mr);
        if(_recv_mr)
            rdma_dereg_mr(_recv_mr);
        if(_recv_buf_selfmanage)
            delete[] _recv_buf;
        if(_send_buf_selfmanage)
            delete[] _send_buf;
        if(_sender_completion_queue)
            ibv_destroy_cq(_sender_completion_queue);
        if(_recver_completion_queue)
            ibv_destroy_cq(_recver_completion_queue);
        if(_queue_pair) {
            ibv_destroy_qp(_queue_pair);
        }
        if(_protection_domain)
            ibv_dealloc_pd(_protection_domain);
        if(_dev_context)
            ibv_close_device(_dev_context);
        if(_device_list)
            ibv_free_device_list(_device_list);
    }

    char* get_send_buffer(char **pbuf, int *size) {
        *pbuf = _send_buf;
        size  = size;
        return _send_buf;
    }

    char* get_recv_buffer(char **pbuf, int *size) {
        *pbuf = _recv_buf;
        size  = size;
        return _recv_buf;
    }

    char* set_send_buffer(char *buf, int size) {
        // OUT("registering _send_buf: %p", buf);
        ASSUME(_protection_domain, "protection domain hasn't been created. connect() or accept() first.");
        if(_send_buf != buf || _send_buf_size < size) {
            if(_send_mr) {
                rdma_dereg_mr(_send_mr);
            }
            // reallocate
            if(_send_buf_selfmanage)
                delete[] _send_buf;
            if(!buf) {
                _send_buf = new char[size];
                _send_buf_selfmanage = true;

            }
            else {
                _send_buf = buf;
                _send_buf_selfmanage = false;
            }
            _send_buf_size = size;

            _send_mr = ibv_reg_mr(_protection_domain, _send_buf, size, IBV_ACCESS_LOCAL_WRITE);
            if (!_send_mr) {
                ERR("ibv_reg_mr for send_buf");
                return nullptr;
            }

            if (!_send_sge) {
                _send_sge = (struct ibv_sge*)calloc(1, sizeof(struct ibv_sge));
            }
            _send_sge->addr   = (uint64_t)_send_buf;
            _send_sge->length = _send_buf_size;
            _send_sge->lkey   = _send_mr->lkey;
        }
        return _send_buf;
    }

    char* set_recv_buffer(char *buf, int size) {
        ASSUME(_protection_domain, "protection domain hasn't been created. connect() or accept() first.");
        if(_recv_buf != buf || _recv_buf_size < size) {
            if(_recv_mr) {
                rdma_dereg_mr(_recv_mr);
            }
            // reallocate
            if(_recv_buf_selfmanage)
                delete[] _recv_buf;
            if(!buf) {
                _recv_buf = new char[size];
                _recv_buf_selfmanage = true;
            }
            else {
                _recv_buf = buf;
                _recv_buf_selfmanage = false;
            }
            _recv_buf_size = size;
            
            // OUT("registering _recv_buf: %p， size=%d, to pd=%p", buf, size, _protection_domain);
            _recv_mr = ibv_reg_mr(_protection_domain, _recv_buf, size, IBV_ACCESS_LOCAL_WRITE);            
            if (!_recv_mr) {
                ERR("ibv_reg_mr for _recv_buf");
                ALWAYS_BREAK();
                return nullptr;
            }
            if (!_recv_sge) {
                _recv_sge = (struct ibv_sge*)calloc(1, sizeof(struct ibv_sge));
            }
            _recv_sge->addr   = (uint64_t)_recv_buf;
            _recv_sge->length = _recv_buf_size;
            _recv_sge->lkey   = _recv_mr->lkey;
        }
        return _recv_buf;
    }

    int bind(char *_lname, char *_lport) {
        return 0;
    }

    int listen() override {
        return 0;
    }


    virtual int send(char *buf, int count, int timeout=-1) {
        if(_send_buf != buf || _send_buf_size < count) {
            set_send_buffer(buf, count);
        }
        return _send(count, _sender_stats.at(0), timeout, _roundtrip);
    }
    virtual int _send(size_t count, fstat & ss_sender, int timeout=-1, bool roundtrip=false) {
        thread_local static TimePoint start;
        int ret;
        uint64_t nano, outstanding = _outstanding_recvs;

        start = CounterClock::now();

        if(post_send(count)<0) {
            return -1;
        }
        // OUT("_send: start polling");
        while(_outstanding_sends) {
            if((CounterClock::now() - start).count()>(uint64_t(timeout)*1000*1000)) {
                OUT("_send: polling timeout. timeout=%d _outstanding_sends=%d _outstanding_recvs=%d (CounterClock::now() - starttime).count()=%zu", timeout, _outstanding_sends.load(), _outstanding_recvs.load(), (CounterClock::now() - start).count());
                errno = ETIMEDOUT;
                return 0;
            }
        }

        if(roundtrip) {
            // recving roundtrip packet
            if((ret = _recv(count, timeout, false))<0) {
                return ret;
            }
        }
        /* statistics of roundtrip time */
        nano = (CounterClock::now() - start).count();
        if(nano>ss_sender.max_lat_nano) {
            ss_sender.max_lat_nano = nano;
        }
        else if(nano<ss_sender.min_lat_nano) {
            ss_sender.min_lat_nano = nano;
        }
        ss_sender.tot_lat_nano += nano;
        ss_sender.op_count++;
        ss_sender.nsent += _send_buf_size;

        return count;
    }

    int recv(char *buf, size_t count, int timeout=-1) {
        if(_recv_buf != buf || _recv_buf_size < count) {
            set_recv_buffer(buf, count);
        }
        return _recv(count, timeout, _roundtrip);
    }
    int _recv(size_t count, int timeout, bool roundtrip) {
        uint64_t nano=0;
        int nw = 0;
        int ret, nwr=0;
        uint64_t outstanding = _outstanding_recvs;

        auto start = CounterClock::now();

        if(post_recv(count)<0) {
            ERR("failed post_recv.");
            return -1;
        }
        // OUT("_recv: start polling");
        while(_outstanding_recvs) {
            if((CounterClock::now() - start).count()>(uint64_t(timeout)*1000*1000)) {
                // OUT("_recv: polling timeout. _outstanding_recvs=%d _outstanding_sends=%d timeout=%d (CounterClock::now() - starttime).count()=%zu", _outstanding_recvs.load(), _outstanding_sends.load(), timeout, (CounterClock::now() - start).count());
                errno = ETIMEDOUT;
                return 0;
            }
        }
        // if((ret = poll_recver_completion(timeout, 1))<=0) {
        //     OUT("ret=%d", ret);
        //     return ret;
        // }

        if(roundtrip) {
            // send roundtrip packet
            if((ret = _send(count, _sender_stats.at(0), timeout, false))<0) {
                ERR("_recv: failed to send roundtrip packet.");
                return ret; 
            }
        }
        /* statistics of recv time */
        nano = (CounterClock::now() - start).count();
        if(nano>_recver_stat.max_lat_nano) {
            _recver_stat.max_lat_nano = nano;
        }
        else if(nano<_recver_stat.min_lat_nano) {
            _recver_stat.min_lat_nano = nano;
        }
        _recver_stat.tot_lat_nano += nano;
        _recver_stat.nrecv        += _recv_buf_size;
        _recver_stat.op_count++;

        return count;
    }

    void sender_poller() {
        struct ibv_wc wc[10] = {{0}};
        int result = 0;
        uint64_t nano = 0;
        fstat & ss_sender = _sender_stats.at(0);

        using ns    = std::chrono::nanoseconds;
        auto now  = CounterClock::now();
        for(;;) {
            if(_quit) return;

            result = ibv_poll_cq(_sender_completion_queue, 1, wc);
            
            if(!result) continue;
            
            // OUT("_outstanding_sends=%d result=%d poll_time=%dnano",_outstanding_sends.load(), result, (CounterClock::now()-now).count());

            _outstanding_sends -= result;

            // if(!_roundtrip) {
            //     // caller would do the stat if roundtrip
            //     now = CounterClock::now();
            //     for(int i=0; i<result; i++) {
            //         if (wc[i].status != ibv_wc_status::IBV_WC_SUCCESS) {
            //             ERR("failed polling from completion queue.");
            //             return;
            //         }
            //         nano = now.time_since_epoch().count() - wc[i].wr_id;
            //         /* statistics of send time */
            //         if(nano>ss_sender.max_lat_nano) {
            //             ss_sender.max_lat_nano = nano;
            //         }
            //         else if(nano<ss_sender.min_lat_nano) {
            //             ss_sender.min_lat_nano = nano;
            //         }
            //         ss_sender.tot_lat_nano += nano;
            //         ss_sender.op_count++;
            //         ss_sender.nsent += _send_buf_size;
            //     }
            // }
        }
    }
    void recver_poller() {
        struct ibv_wc wc[10] = {{0}};
        int result = 0;
        uint64_t nano = 0;

        using ns    = std::chrono::nanoseconds;
        auto now  = CounterClock::now();
        for(;;) {
            if(_quit) return;

            // if(_outstanding_recvs<=2) {
            //     // make sure the receiver wr queue is never empty
            //     // otherwise a RNR (receiver not ready) will be triggerred
            //     for(int i=0; _outstanding_recvs<10; i++)
            //         post_recv(_recv_buf_size);
            // }
            
            result = ibv_poll_cq(_recver_completion_queue, 1, wc);
            if(!result) continue;
            
            // OUT("_outstanding_recvs=%d result=%d poll_time=%dnano",_outstanding_recvs.load(), result, (CounterClock::now()-now).count());

            _outstanding_recvs -= result;
            // if(!_roundtrip) {
            //     now = CounterClock::now();
            //     for(int i=0; i<result; i++) {
            //         if (wc[i].status != ibv_wc_status::IBV_WC_SUCCESS) {
            //             ERR("failed polling from completion queue.");
            //             return;
            //         }
            //         nano = now.time_since_epoch().count() - wc[i].wr_id;

            //         /* statistics of recv time */
            //         if(nano>_recver_stat.max_lat_nano) {
            //             _recver_stat.max_lat_nano = nano;
            //         }
            //         else if(nano<_recver_stat.min_lat_nano) {
            //             _recver_stat.min_lat_nano = nano;
            //         }
            //         _recver_stat.tot_lat_nano += nano;
            //         _recver_stat.nrecv        += _recv_buf_size;
            //         _recver_stat.op_count++;
            //     }
            // }
        }
    }

    int send_submit(char *buf, int count, int timeout=0) final {
        using    Clock = std::chrono::high_resolution_clock;
        auto     start = CounterClock::now();
        auto     dur   = CounterClock::now() - start;
        uint64_t nano;

        start = CounterClock::now();

        if(post_send(count)<0) { 
            return -1;
        }

        if(_roundtrip) {
            // recving roundtrip packet
            if(post_recv(count)<0) {
                return -1;
            }
        }

        nano = (CounterClock::now() - start).count();

        if(nano>_submit_stat.max_lat_nano) {
            _submit_stat.max_lat_nano = nano;
        }
        else if(nano<_submit_stat.min_lat_nano) {
            _submit_stat.min_lat_nano = nano;
        }
        _submit_stat.tot_lat_nano += nano;
        _submit_stat.op_count++;

        return count;
    }
    int sender_drain(int timeout=0) final {
        auto start = CounterClock::now();
        while(_outstanding_recvs || _outstanding_sends) {
            if(timeout && (CounterClock::now() - start).count() > timeout*1000000) {
                break;
            }
        }
        return 0;
    }

    operator int() final {
        return 0;
    }

    struct addrinfo _addrhints_for_bind() final {}
    struct addrinfo _addrhints_for_conn() final {}
    int _accept(char *_rname, char* _rport) final {}
    int _connect(char *_rname, char* _rport, int timeout=-1) final {}
};

using f_stream_rdma = f_stream_rdma_ibverbs;
#else
using f_stream_rdma = f_stream_tcp;
#endif