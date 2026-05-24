#ifndef __M_CONNECTION_H__
#define __M_CONNECTION_H__
#include "muduo/proto/dispatcher.h"
#include "muduo/proto/codec.h"
#include "muduo/base/Logging.h"
#include "muduo/base/Mutex.h"
#include "muduo/net/EventLoop.h"
#include "muduo/net/TcpClient.h"
#include "muduo/net/EventLoopThread.h"
#include "muduo/base/CountDownLatch.h"

#include "channel.hpp"
#include "worker.hpp"

namespace mymq {
class Connection {
    public:
        using ptr = std::shared_ptr<Connection>;

        Connection(const std::string &sip, int sport, const AsyncWorker::ptr &worker):
            _latch(1), _client(worker->loopthread.startLoop(), muduo::net::InetAddress(sip, sport), "Client"),
            _dispatcher(std::bind(&Connection::onUnknownMessage, this, std::placeholders::_1, 
                std::placeholders::_2, std::placeholders::_3)),
            _codec(std::make_shared<ProtobufCodec>( std::bind(&ProtobufDispatcher::onProtobufMessage, &_dispatcher, 
                std::placeholders::_1, std::placeholders::_2, std::placeholders::_3))),
            _worker(worker),
            _channel_manager(std::make_shared<ChannelManager>()){

            _dispatcher.registerMessageCallback<mq::basicCommonResponse>(std::bind(&Connection::basicResponse, this, 
                std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
                
            _dispatcher.registerMessageCallback<mq::basicConsumeResponse>(std::bind(&Connection::consumeResponse, this, 
                std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
            //
            _client.setMessageCallback(std::bind(&ProtobufCodec::onMessage, _codec.get(),
                std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
            //建立连接时客户端调用的函数
            _client.setConnectionCallback(std::bind(&Connection::onConnection, this, std::placeholders::_1));      

            
            _client.connect();
            _latch.wait();//阻塞等待，直到连接建立成功
        }

        Channel::ptr openChannel() {
            Channel::ptr channel = _channel_manager->create(_conn, _codec);
            //给服务器发，客服端要在连接上创建一个信道。因为服务器和客户端通过一个信道来传递消息
            bool ret = channel->openChannel();
            if (ret == false) {
                DLOG("打开信道失败！");
                return Channel::ptr();
            }
            return channel;
        }
        void closeChannel(const Channel::ptr &channel) {
            //给服务器发关闭信道的消息 
            channel->closeChannel();
            _channel_manager->remove(channel->cid());
        }
    private:

        //收到基础响应的回调函数
        void basicResponse(const muduo::net::TcpConnectionPtr& conn, const basicCommonResponsePtr& message, muduo::Timestamp) {
            //1. 找到信道
            Channel::ptr channel = _channel_manager->get(message->cid());
            if (channel.get() == nullptr) {
                DLOG("未找到信道信息！");
                return;
            }
            //2. 将得到的响应对象，添加到信道的基础响应hash_map中
            channel->putBasicResponse(message);
        }

        //收到消息推送的回调函数
        void consumeResponse(const muduo::net::TcpConnectionPtr& conn, const basicConsumeResponsePtr& message, muduo::Timestamp){
            //1. 找到信道
            Channel::ptr channel = _channel_manager->get(message->cid());
            if (channel.get() == nullptr) {
                DLOG("未找到信道信息！");
                return;
            }
            //2. 封装异步任务（消息处理任务），抛入线程池
            _worker->pool.push([channel, message](){
                channel->consume(message);
            });
        }
        void onUnknownMessage(const muduo::net::TcpConnectionPtr& conn, const MessagePtr& message, muduo::Timestamp) {
            LOG_INFO << "onUnknownMessage: " << message->GetTypeName();
            conn->shutdown();
        }
        void onConnection(const muduo::net::TcpConnectionPtr&conn){
            if (conn->connected()) {
                _latch.countDown();//唤醒主线程中的阻塞
                _conn = conn;
            }else {
                //连接关闭时的操作
                _conn.reset();
            }
        }
    private:
        muduo::CountDownLatch _latch;//实现同步的
        muduo::net::TcpConnectionPtr _conn;//客户端对应的连接
        muduo::net::TcpClient _client;//客户端
        ProtobufDispatcher _dispatcher;//请求分发器
        ProtobufCodecPtr _codec;//协议处理器

        AsyncWorker::ptr _worker;
        ChannelManager::ptr _channel_manager;
};
}

#endif