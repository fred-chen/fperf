/**
 * @author Fred Chen
 * @email  fred.chen@live.com
 * @create date 2021-10-05 17:03:09
 * @modify date 2021-10-05 17:03:09
 */

#include <fperf.hpp>
#include <getopt.h>
#include <atomic>

bool fparseopts(int argc, char** argv, fsettings *config);
int  server();
int  client(fsettings *config);
void server_report(std::vector<f_stream_interface*> &streams, const fsettings *config);

int main(int argc, char** argv) {
    // setprinttime(true);
    fsettings config;
    fparseopts(argc, argv, &config);
    if(config.settings.opcode == 0) {
        server();
    }
    else {
        client(&config);
    }
    return 0;
}

/**
 * @brief SERVER, infinity loop
 * 
 * @param config 
 * @return int -1 on error, 0 succeed
 */
int server() {
    int  err;
    char **buf_array = nullptr;
    fsettings config;
    std::vector<f_stream_interface*> v_streams;
    std::vector<std::thread*>        v_svr_threads;

    while(true) {
        // server loop starts here
        // create control stream with TCP mode
        // OUT("Server creating stream_control");
        f_stream_tcp stream_control(nullptr, CTRL_PORT, nullptr, nullptr);


        // OUT("Server stream_control entering passive mode");
        printf("\nServer listening .... ");
        ::fflush(stdout);

        // block and listen for new test request
        if(stream_control.enter_passive_mode()<0) {
            ERR("control socket failed to enter passive mode.");
            return -1;
        }

        // OUT("Server stream_control receive client settings");
        // receive client settings
        if((err = stream_control.recv((char*)&config.settings, sizeof(struct fsettings_t)))<0) {
            ERR("exchange client settings.");
            return -1;
        }

        // safe_print(string(config));
        if(config.settings.nstreams>1) {
            printf("\n");
        }
        delete[] buf_array;
        buf_array = new char*[config.settings.nstreams];
        // create streams according to client's request
        for(int i=0; i<config.settings.nstreams; i++) {
            f_stream_interface *stream;

            // change buffer size to the same as client
            buf_array[i] = new char[config.settings.pktsize];
            
            // create data stream
            if(config.settings.prot == PROTTCP) {
                stream = new f_stream_tcp(nullptr, std::to_string(atoi(RECVER_PORT)+i).c_str(), nullptr, nullptr, config.settings.qtype, config.settings.nsockets, config.settings.roundtrip, config.settings.nodelay, config.settings.quickack);
            }
            else if (config.settings.prot == PROTUDP) {
                stream = new f_stream_udp(nullptr, std::to_string(atoi(RECVER_PORT)+i).c_str(), nullptr, nullptr, config.settings.qtype, config.settings.nsockets, config.settings.roundtrip);
            }
            else {
            #ifdef AGM_RDMA
                stream = new f_stream_rdma(nullptr, std::to_string(atoi(RECVER_PORT)+i).c_str(), nullptr, nullptr, config.settings.qtype, config.settings.nsockets, config.settings.roundtrip, 0, 0, &stream_control, config.settings.rdmadev);
            #else
                // if the platform doesn't support RDMA, use TCP instead
                stream = new f_stream_tcp(nullptr, std::to_string(atoi(RECVER_PORT)+i).c_str(), nullptr, nullptr, config.settings.qtype, config.settings.nsockets, config.settings.roundtrip, config.settings.nodelay, config.settings.quickack);
            #endif
            }

            // bind data stream
            if((err = stream->bind()) < 0) {
                ERR("failed in bind.");
                delete stream;
                return err;
            }

            // listen and accept data stream
            if((err = stream->listen()) < 0) {
                ERR("failed in listen.");
                delete stream;
                return err;
            }

            // tell the client we are ready for accepting data stream connections
            // wait for the client to be ready by receving ack from client
            // on success, client should send 'ready_too' as response
            char q[] = "ready";
            char a[] = "ready_too";
            if(stream_control.send(q, sizeof(q)) != sizeof(q)) {
                ERR("failed sending 'ready' to client");
            }
            if(!stream_control.strexpect(a)) {
                ERR("failed sending ack to client.");
                delete stream;
                return -1;
            }

            // block and wait for data stream connection
            if((err = stream->accept()) < 0) {
                ERR("failed in accept.");
                delete stream;
                return err;
            }

            v_streams.push_back(stream);

            char stream_type[5];
            if(stream->prot==PROTTCP) {
                strcpy(stream_type, "TCP");
            }
            else if(stream->prot==PROTUDP) {
                strcpy(stream_type, "UDP");
            }
            else {
                strcpy(stream_type, "RDMA");
            }

            safe_print("new %s connection from %s:%d. pkt_size(%dB)",
                stream_type, 
                stream->str_peer_address().c_str(), stream->peer_port(),
                config.settings.pktsize);
        }

        // OUT("start receving data");
        // therad function for receiving data
        auto svr = [&config](f_stream_interface* stream, char* buf) {
            int nr;

            while(true) {
                // OUT("start recv");
                nr = stream->recv(buf, config.settings.pktsize, 600);
                // OUT("data received n=%d", n);

                if(nr<=0) {
                    // ERR("error receving data n=%d", n);
                    if(errno==ETIMEDOUT || (errno==0 && nr==0) || errno==ECONNRESET || errno==EAGAIN || errno==EWOULDBLOCK) {
                        // the socket isn't ready for read
                        // this may be caused by a client close
                        // stream->reset_recver_stat();  // not necessary, just for clear

                        // in case of TCP
                        // the socket might be closed by peer
                        // so the read meets EOF and returns 0
                        if(stream->prot == PROTTCP)
                            stream->close();

                        // break and start over for new test
                        // OUT("server: deleting data stream");
                        break;
                    }
                    else {
                        // real error happened during data transfer
                        // but some weird cases, the n is a large minus number
                        // and the errno is still 0 here... like this:
                        // ERR: Server: failed to receive. n=-540434528 errno=0(Success)
                        if(errno != 0)
                            ERR("Server: failed to receive. n=%d", nr);
                        break;
                    }
                }
                // OUT("next recv");
            }
            // OUT("server: n_did_recve=%zuB(%zuMB)", stream->n_did_recve.load(), stream->n_did_recve.load()/1024/1024);
        };
        // now the data transfer starts
        int i = 0;
        for(auto stream : v_streams) {
            char *buf = buf_array[i++];
            #ifdef AGM_RDMA
            if(config.settings.prot == PROTRDMA) {
                if(!((f_stream_rdma*)stream)->set_recv_buffer(buf, config.settings.pktsize)) {
                    return -1;
                }
                if(!((f_stream_rdma*)stream)->set_send_buffer(buf, config.settings.pktsize)) {
                    return -1;
                }
            }
            #endif
            std::thread *pth = new std::thread(svr, stream, buf);
            v_svr_threads.push_back(pth);
        }

        for(auto pth : v_svr_threads) {
            pth->join();
        }
        // report and start it over
        server_report(v_streams, &config);
        for(auto stream : v_streams) {
            delete stream;
        }
        v_svr_threads.clear();
        v_streams.clear();
        // OUT("server: starting over");
    }

    // server never stops until control-c or error
    return 0;
}

/* 
 *     server report
 */
void server_report(std::vector<f_stream_interface*> &streams, const fsettings *config) {

    double tot_lat       = 0;
    double avg_lat       = 0;
    double min_lat       = INT_MAX;
    double max_lat       = 0;
    size_t nrecv         = 0;
    size_t op_count      = 0;
    int    nstreams      = 0;
    for(auto stream : streams) {
        fstat _recver_stats = stream->get_recver_stat();
        tot_lat            += _recver_stats.tot_lat_nano;
        min_lat             = _recver_stats.min_lat_nano<min_lat?_recver_stats.min_lat_nano.load():min_lat;
        max_lat             = _recver_stats.max_lat_nano>max_lat?_recver_stats.max_lat_nano.load():max_lat;
        nrecv              += _recver_stats.nrecv;
        op_count           += _recver_stats.op_count;
        nstreams++;
    }
    avg_lat = op_count==0 ? 0 : tot_lat/op_count;

    double ms_total      = double(tot_lat)/nstreams/1000/1000;
    double seconds_total = double(tot_lat)/nstreams/1000/1000/1000;
    double mb_total      = double(nrecv)/1024/1024;
    double gb_total      = double(nrecv)/1024/1024/1024;
    double mbit_per_s    = mb_total*8/seconds_total;
    double gbit_per_s    = mbit_per_s/1024;

    safe_print("\nreceived : %.0fMB(%.2fGB %zuB) in %.*f%s. (bw: %.*f%s/s %.*f%s/s pps: %.0f) ", 
                mb_total, gb_total, nrecv, 
                seconds_total>=1?2:0, seconds_total>=1?seconds_total:ms_total, seconds_total>=1?"s":"ms",
                gb_total>1?2:0, gb_total>1?gb_total/seconds_total:mb_total/seconds_total, gb_total>1?"GB":"MB",
                gbit_per_s>1?2:0, gbit_per_s>1?gbit_per_s:mbit_per_s, gbit_per_s>1?"Gbit":"Mbit",
                double(op_count)/seconds_total);
    safe_print("%s avg=%.*f%s    min=%.*f%s    max=%.*f%s",
                config->settings.roundtrip?"mprtt    :":"latency  :",
                avg_lat>1000?2:0, avg_lat>1000?avg_lat/1000:avg_lat, avg_lat>1000?"us":"ns",
                min_lat>1000?2:0, min_lat>1000?min_lat/1000:min_lat, min_lat>1000?"us":"ns",
                max_lat>1000?2:0, max_lat>1000?max_lat/1000:max_lat, max_lat>1000?"us":"ns");

    if(streams.size()>1) {
        safe_print("Per Stream:");
        for(auto stream : streams) {
            fstat _recver_stats = stream->get_recver_stat();
            tot_lat             = _recver_stats.tot_lat_nano;
            min_lat             = _recver_stats.min_lat_nano<min_lat?_recver_stats.min_lat_nano.load():min_lat;
            max_lat             = _recver_stats.max_lat_nano>max_lat?_recver_stats.max_lat_nano.load():max_lat;
            nrecv               = _recver_stats.nrecv;
            op_count            = _recver_stats.op_count;
            ms_total            = double(tot_lat)/1000/1000;
            seconds_total       = double(ms_total)/1000;
            mb_total            = double(nrecv)/1024/1024;
            gb_total            = double(nrecv)/1024/1024/1024;
            double mbit_per_s   = mb_total*8/seconds_total;
            double gbit_per_s   = mbit_per_s/1024;
            safe_print("  stream %2d %s avg_lat=%.*f%s min_lat=%.*f%s max_lat=%.*f%s sent: %.*f%s bw: %.*f%s/s pps: %.0f", 
                int(*stream), config->settings.roundtrip?"mprtt:":"latency:",
                avg_lat>1000?2:0, avg_lat>1000?avg_lat/1000:avg_lat, avg_lat>1000?"us":"ns",
                min_lat>1000?2:0, min_lat>1000?min_lat/1000:min_lat, min_lat>1000?"us":"ns",
                max_lat>1000?2:0, max_lat>1000?max_lat/1000:max_lat, max_lat>1000?"us":"ns",
                gb_total>1?2:0, gb_total>1?gb_total:mb_total,gb_total>1?"GB":"MB",
                gbit_per_s>1?2:0, gbit_per_s>1?gbit_per_s:mbit_per_s, gbit_per_s>1?"Gbit":"Mbit",
                double(op_count)/seconds_total);
        }
    }
}

/**
 * @brief CLIENT
 * 
 * @param config 
 * @return int 
 */
int client(fsettings *config) {
    int  err;
    int  n;
    char *buf = longstr(config->settings.pktsize);
    f_stream_tcp stream_control(nullptr, nullptr, config->settings.ip, CTRL_PORT);
    f_stream_interface *stream_client;
    std::vector<f_stream_interface*> v_streams;

    // safe_print("CLIENT CONFIG:\n%s", string(*config).c_str());

    // create control stream with TCP mode
    if(stream_control.connect()<0) {
        ERR("control socket failed to enter active mode.");
        return -1;
    }

    // OUT("client sending settings.");
    // exchange client settings
    if((n = stream_control.send((char*)&config->settings, sizeof(struct fsettings_t)))<0) {
        ERR("exchange client settings.");
        return -1;
    }

    // OUT("client stream_control connected. %s:%d", stream_control.str_local_address().c_str(), stream_control.local_port());

    for(int i=0; i<config->settings.nstreams; i++) {
        // OUT("client waiting for acks.");
        // wait for ack from server
        char q[] = "ready";
        char a[] = "ready_too";
        if(!(err = stream_control.strexpect(q, a))) {
            ERR("failed recv ack.");
            return err;
        }

        // give the server some time 
        // to start accepting data stream connection
        SLEEP_MS(1); 

        // OUT("client creating data stream.");
        if(config->settings.prot == PROTTCP) {
            stream_client = new f_stream_tcp(nullptr, nullptr, config->settings.ip, std::to_string(atoi(RECVER_PORT)+i).c_str(), 
                config->settings.qtype, config->settings.nsockets, config->settings.roundtrip, config->settings.nodelay, config->settings.quickack);
        }
        else if (config->settings.prot == PROTUDP) {
            stream_client = new f_stream_udp(nullptr, nullptr, config->settings.ip, std::to_string(atoi(RECVER_PORT)+i).c_str(), 
                config->settings.qtype, config->settings.nsockets, config->settings.roundtrip);
        }
        #ifdef AGM_RDMA
        else {
            stream_client = new f_stream_rdma(nullptr, nullptr, config->settings.ip, std::to_string(atoi(RECVER_PORT)+i).c_str(), 
                config->settings.qtype, config->settings.nsockets, config->settings.roundtrip, 0, 0, &stream_control, config->settings.rdmadev);
        }
        #endif

        // OUT("client data stream connecting connecting.");
        err = stream_client->connect();
        if(err < 0) {
            delete stream_client;
            return err;
        }

        char stream_type[5];
        if(stream_client->prot==PROTTCP) {
            strcpy(stream_type, "TCP");
        }
        else if(stream_client->prot==PROTUDP) {
            strcpy(stream_type, "UDP");
        }
        else {
            strcpy(stream_type, "RDMA");
        }

        if(config->settings.nsockets==1) {
            safe_print("%s STREAM %d %s:%d => %s:%d pkt_size=%dB sync(no-queue) nsockets=%d", 
                stream_type, int(*stream_client), stream_client->str_local_address().c_str(), stream_client->local_port(),
                stream_client->str_peer_address().c_str(), stream_client->peer_port(),
                config->settings.pktsize, config->settings.nsockets);
        }
        else {
            safe_print("%s STREAM %d %s:%d => %s:%d pkt_size=%dB queue_type=%s nsockets=%d",
                stream_type, int(*stream_client), stream_client->str_local_address().c_str(), stream_client->local_port(),
                stream_client->str_peer_address().c_str(), stream_client->peer_port(),
                config->settings.pktsize, config->settings.qtype==QTMUTEX?"'MUTEX'":"'LOCKFREE'", config->settings.nsockets);
        }
        v_streams.push_back(stream_client);
    }

    // client thread function
    auto client = [&config, &n, &buf](f_stream_interface* stream) -> void {
        size_t nleft = config->settings.size;
        size_t nsending;
        #ifdef AGM_RDMA
        if(config->settings.prot == PROTRDMA) {
            if(!((f_stream_rdma*)stream)->set_recv_buffer(buf, config->settings.pktsize)) {
                return;
            }
            if(!((f_stream_rdma*)stream)->set_send_buffer(buf, config->settings.pktsize)) {
                return;
            }
        }
        #endif
        while(nleft>0) {
            nsending = nleft < config->settings.pktsize ? nleft : config->settings.pktsize;
            if(config->settings.nsockets==1 && config->settings.qtype==1) {
                // no multiplex, just send synchronously
                n = stream->send(buf, nsending, 600);
            }
            else {
                n = stream->send_submit(buf, nsending, 600);
            }
            if(n<0) {
                ERR("CLIENT: failed sending packet. ret=%d", n);
                break;
            }
            if(n==0 && errno==ETIMEDOUT) {
                //timeout
                break;
            }
            nleft -= n;
        }
        stream->sender_drain(500);
        // OUT("n_to_send  = %zuB(%zuMB)\nn_did_send = %zuB(%zuMB)", stream->n_to_send.load(), stream->n_to_send.load()/1024/1024, stream->n_did_send.load(), stream->n_did_send.load()/1024/1024);
    };

    // start client threads
    std::vector<std::thread*> v_cli_threads;
    for(auto stream : v_streams) {
        std::thread *pth = new std::thread(client, stream);
        v_cli_threads.push_back(pth);
    }

    for(auto pth : v_cli_threads) {
        pth->join();
    }
    
    /* 
     *                       client report
     */

    // global stats report
    // aggregation of latencies
    double   tot_lat  = 0;
    double   min_lat  = INT_MAX;
    double   max_lat  = 0;
    uint64_t op_count = 0;
    uint64_t nsent    = 0;
    uint64_t nrecv    = 0;
    uint64_t nrecv_op = 0;
    int      nthreads = 0;
    double   tot_lat_submit  = 0;
    double   min_lat_submit  = INT_MAX;
    double   max_lat_submit  = 0;
    uint64_t op_count_submit = 0;

    for(auto stream : v_streams) {
        fstat submit_stat                 = stream->get_submit_stat();
        std::map<int, fstat> sender_stats = stream->get_sender_stats();
        fstat recver_stat                 = stream->get_recver_stat();
        nthreads                         += sender_stats.size();
        nrecv                            += recver_stat.nrecv;
        nrecv_op                         += recver_stat.op_count;
        for (auto pair : sender_stats) {
            auto ss = pair.second;
            tot_lat  += ss.tot_lat_nano;
            min_lat   = ss.min_lat_nano < min_lat ? ss.min_lat_nano.load() : min_lat;
            max_lat   = ss.max_lat_nano > max_lat ? ss.max_lat_nano.load() : max_lat;
            op_count += ss.op_count;
            nsent    += ss.nsent;
        }
        tot_lat_submit  += submit_stat.tot_lat_nano;
        min_lat_submit   = submit_stat.min_lat_nano<min_lat_submit ? submit_stat.min_lat_nano.load() : min_lat_submit;
        max_lat_submit   = submit_stat.max_lat_nano>max_lat_submit ? submit_stat.max_lat_nano.load() : max_lat_submit;
        op_count_submit += submit_stat.op_count;
    }
    double avg_lat        = op_count==0?0:double(tot_lat)/op_count;
    double ms_total       = double(tot_lat)/1000/1000/nthreads;
    double seconds_total  = double(ms_total)/1000;
    double mb_total       = double(nsent)/1024/1024;
    double gb_total       = double(nsent)/1024/1024/1024;
    double avg_lat_submit = op_count_submit == 0 ? 0 : tot_lat_submit/op_count_submit;
    double mbit_per_s = mb_total*8/seconds_total;
    double gbit_per_s = mbit_per_s/1024;

    safe_print("\nsent      : %.0fMB(%.2fGB %zuB) in %.*f%s. (bw: %.*f%s/s %.*f%s/s pps: %.0f)", 
                mb_total, gb_total, nsent, 
                seconds_total>=1?2:0, seconds_total>=1?seconds_total:ms_total, seconds_total>=1?"s":"ms",
                gb_total>1?2:0, 
                gb_total>1?gb_total/seconds_total:mb_total/seconds_total, gb_total>1?"GB":"MB", 
                gbit_per_s>1?2:0, gbit_per_s>1?gbit_per_s:mbit_per_s, gbit_per_s>1?"Gbit":"Mbit",
                double(op_count)/seconds_total);

    if(config->settings.roundtrip) {
        mb_total      = double(nrecv)/1024/1024;
        gb_total      = double(nrecv)/1024/1024/1024;
        safe_print("received  : %.0fMB(%.2fGB %zuB) pps: %.0f", mb_total, gb_total, nrecv,
            double(nrecv_op)/seconds_total);
    }

    safe_print("%s avg=%.*f%s    min=%.*f%s    max=%.*f%s",
                config->settings.roundtrip?"mprtt     :":"latency   :",
                avg_lat>1000?2:0, avg_lat>1000?avg_lat/1000:avg_lat, avg_lat>1000?"us":"ns",
                min_lat>1000?2:0, min_lat>1000?min_lat/1000:min_lat, min_lat>1000?"us":"ns",
                max_lat>1000?2:0, max_lat>1000?max_lat/1000:max_lat, max_lat>1000?"us":"ns");
    
    if(config->settings.nsockets>1 || config->settings.qtype==0) {
        // if multiple socks, then it goes async path ( with a queue )
        // with this mode, the queue submit time is in the statistics
        // if qtype 0 (lockfree) specified, even the nsockets is 1,
        // the client still goes async path, so print submit lat anyway
        safe_print("lat_submit: avg=%.0fns    min=%.0fns    max=%.0fns", 
            avg_lat_submit, min_lat_submit, max_lat_submit);
    }

    if(config->settings.nstreams>1 || config->settings.nsockets>1) {
        // if multiple streams, print per stream statistics
        safe_print("\nPer Stream:");
        for(auto stream : v_streams) {
            std::map<int, fstat> sender_stats = stream->get_sender_stats();
            fstat recver_stat                 = stream->get_recver_stat();
            nrecv                             = recver_stat.nrecv;
            tot_lat  = 0;
            min_lat  = INT_MAX;
            max_lat  = 0;
            op_count = 0;
            nsent    = 0;
            nthreads = 0;
            for (auto pair : sender_stats) {
                // OUT("socket: %d, ss.op_count=%d", pair.first, ss.op_count.load());
                auto ss   = pair.second;
                tot_lat  += ss.tot_lat_nano;
                min_lat   = ss.min_lat_nano < min_lat ? ss.min_lat_nano.load() : min_lat;
                max_lat   = ss.max_lat_nano > max_lat ? ss.max_lat_nano.load() : max_lat;
                op_count += ss.op_count;
                nsent    += ss.nsent;
                nthreads++;
            }
            avg_lat         = op_count==0?0:double(tot_lat)/op_count;
            ms_total        = double(tot_lat)/1000/1000/nthreads;
            seconds_total   = double(ms_total)/1000;
            mb_total        = double(nsent)/1024/1024;
            gb_total        = double(nsent)/1024/1024/1024;
            double mbit_per_s = mb_total*8/seconds_total;
            double gbit_per_s = mbit_per_s/1024;
            avg_lat_submit = op_count_submit == 0 ? 0 : tot_lat_submit/op_count_submit;
            safe_print("  stream %-2d %s avg=%.*f%s    min=%.*f%s    max=%.*f%s sent: %.*f%s  bw: %.*f%s/s pps: %.0f",
                        int(*stream),config->settings.roundtrip?"mprtt:":"latency:",
                        avg_lat>1000?2:0, avg_lat>1000?avg_lat/1000:avg_lat, avg_lat>1000?"us":"ns",
                        min_lat>1000?2:0, min_lat>1000?min_lat/1000:min_lat, min_lat>1000?"us":"ns",
                        max_lat>1000?2:0, max_lat>1000?max_lat/1000:max_lat, max_lat>1000?"us":"ns",
                        gb_total>1?2:0, gb_total>1?gb_total:mb_total,gb_total>1?"GB":"MB",
                        gbit_per_s>1?2:0, gbit_per_s>1?gbit_per_s:mbit_per_s, gbit_per_s>1?"Gbit":"Mbit",
                        double(op_count)/seconds_total);
            if(config->settings.nsockets>1) {
                // per-socket statistics
                std::map<int, fstat> sender_stats = stream->get_sender_stats();
                fstat recver_stat                 = stream->get_recver_stat();
                nthreads                          = sender_stats.size();
                nrecv                             = recver_stat.nrecv;
                tot_lat  = 0;
                min_lat  = INT_MAX;
                max_lat  = 0;
                op_count = 0;
                nsent    = 0;
                for (auto pair : sender_stats) {
                    int sock = pair.first;
                    auto ss  = pair.second;
                    tot_lat  = ss.tot_lat_nano;
                    min_lat  = ss.min_lat_nano < min_lat ? ss.min_lat_nano.load() : min_lat;
                    max_lat  = ss.max_lat_nano > max_lat ? ss.max_lat_nano.load() : max_lat;
                    op_count = ss.op_count;
                    nsent    = ss.nsent;
                    avg_lat        = op_count==0?0:double(tot_lat)/op_count;
                    ms_total       = double(tot_lat)/1000/1000;
                    seconds_total  = double(ms_total)/1000;
                    mb_total       = double(nsent)/1024/1024;
                    gb_total       = double(nsent)/1024/1024/1024;
                    double mbit_per_s = mb_total*8/seconds_total;
                    double gbit_per_s = mbit_per_s/1024;
                    avg_lat_submit = op_count_submit == 0 ? 0 : tot_lat_submit/op_count_submit;
                    safe_print("      sock%2d avg=%.*f%s    min=%.*f%s    max=%.*f%s sent: %.*f%s  bw: %.*f%s/s pps: %.0f",
                                sock,
                                avg_lat>1000?2:0, avg_lat>1000?avg_lat/1000:avg_lat, avg_lat>1000?"us":"ns",
                                min_lat>1000?2:0, min_lat>1000?min_lat/1000:min_lat, min_lat>1000?"us":"ns",
                                max_lat>1000?2:0, max_lat>1000?max_lat/1000:max_lat, max_lat>1000?"us":"ns",
                                gb_total>1?2:0, gb_total>1?gb_total:mb_total,gb_total>1?"GB":"MB",
                                gbit_per_s>1?2:0, gbit_per_s>1?gbit_per_s:mbit_per_s, gbit_per_s>1?"Gbit":"Mbit",
                                double(op_count)/seconds_total);
                }
            }
        }
    }

    for(auto pth : v_cli_threads) {
        delete pth;
    }
    for(auto stream : v_streams) {
        delete stream;
    }
    delete[] buf;
    return 0;
}















/** functions **/

void usage() {
    const char usage_longstr[] = "'fperf' measures UDP/TCP/RDMA bandwidth and latency.\n\n"
                                 "Usage: fperf --server | --client server_name\n"
                                 "  -t, --nstreams n        number of streams\n"
                                 "  -n, --nsockets n        number of sockets per stream\n"
                                 "  -u, --udp               use UDP  (default TCP)\n"
                                 "  -R, --rdma              use RDMA (default TCP)\n"
                                 "  -D, --rdmadev           RDMA NIC name (use the first found by default)\n"
                                 "  -m, --mb n              n MB to send (default 10240 MB)\n"
                                 "  -r, --mprtt             'most-practicle-round-trip-time' "
                                                            "wait for ack before sending next\n"
                                 "  -l, --pktsize           packet size in BYTE (default 32768)\n"
                                 "  -q, --qtype             queue type: 1: mutex queue(default), 0: lockfree queue\n"
                                 "  -d, --nodelay           for TCP only, add TCP_NODELAY flag to disable nagle's algorithm\n"
                                 "  -a, --quickack          for TCP only, add TCP_QUICKACK flag to disable delayed ack algorithm\n";
    safe_print(usage_longstr);
    exit(1);
}

bool fparseopts(int argc, char** argv, fsettings *config) {
    int c;

    while (true)
    {
        static struct option long_options[]   = 
        {
            {"help"      , no_argument      , 0, 'h'},
            {"server"    , no_argument      , 0, 's'},
            {"client"    , required_argument, 0, 'c'},
            {"udp"       , no_argument      , 0, 'u'},
            {"rdma"      , no_argument      , 0, 'R'},
            {"rdmadev"   , required_argument, 0, 'D'},
            {"pktsize"   , required_argument, 0, 'l'},
            {"port"      , required_argument, 0, 'p'},
            {"mprtt"     , no_argument      , 0, 'r'},
            {"mb"        , required_argument, 0, 'm'},
            {"nsockets"  , required_argument, 0, 'n'},
            {"nstreams"  , required_argument, 0, 't'},
            {"qtype"     , required_argument, 0, 'q'},
            {"nodelay"   , no_argument      , 0, 'd'},
            {"quickack"  , no_argument      , 0, 'a'},
            {0, 0, 0, 0}
        };

        /* getopt_long stores the option index here. */
        int option_index = 0;
        c = getopt_long (argc, argv, "h,s,c:,u,p:,r,m:,t:n:q:l:d,a,R,D:", long_options, &option_index);

        /* Detect the end of the options. */
        if (c == -1)
        break;

        switch (c)
        {
            case 'h':
            usage();
            break;

            case 'l':
            config->settings.pktsize  = atoi(optarg);
            break;

            case 'r':
            config->settings.roundtrip = true;
            break;

            case 's':
            config->settings.opcode   = 0;
            break;

            case 'm':
            config->settings.size     = size_t(atoi(optarg))*1024*1024;
            break;

            case 't':
            config->settings.nstreams = atoi(optarg);
            break;

            case 'n':
            config->settings.nsockets = atoi(optarg);
            break;

            case 'c':
            config->settings.opcode   = 1;
            strcpy(config->settings.ip, optarg);
            break;

            case 'u':
            config->settings.prot     = PROTUDP;
            break;

            case 'R':
            config->settings.prot     = PROTRDMA;
            break;

            case 'D':
            strcpy(config->settings.rdmadev, optarg);
            break;

            case 'q':
            config->settings.qtype    = atoi(optarg);
            break;

            case 'd':
            config->settings.nodelay  = true;
            break;

            case 'a':
            config->settings.quickack = true;
            break;

        default:
            usage();
            return false;
        }
    }
    return true;
}
