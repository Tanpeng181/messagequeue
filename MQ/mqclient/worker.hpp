#ifndef __M_WORKER_H__
#define __M_WORKER_H__
#include "muduo/net/EventLoopThread.h"
#include "../mqcommon/logger.hpp"
#include "../mqcommon/helper.hpp"
#include "../mqcommon/threadpool.hpp"

namespace mymq {
    class AsyncWorker {
        public:
            using ptr = std::shared_ptr<AsyncWorker>;
            
            muduo::net::EventLoopThread loopthread;
            threadpool pool;
    };
}

#endif