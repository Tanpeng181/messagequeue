#include "muduo/proto/dispatcher.h"
#include "muduo/proto/codec.h"
#include "request.pb.h"
#include<muduo/net/EventLoop.h>
#include<muduo/net/TcpServer.h>
#include<muduo/net/TcpConnection.h>
#include<muduo/net/InetAddress.h>
#include<muduo/net/EventLoopThread.h>
#include<muduo/base/CountDownLatch.h>
#include <muduo/base/Logging.h>

class Server{
    public:
    typedef std::shared_ptr<google::protobuf::Message> MessagePtr;
    typedef std::shared_ptr<bit::TranslateRequest> TranslateRequestPtr;
    typedef std::shared_ptr<bit::TranslateResponse> TranslateResponsePtr;
    typedef std::shared_ptr<bit::AddRequest> AddRequestPtr;
    typedef std::shared_ptr<bit::AddResponse> AddResponsePtr;
    Server(int port)
        : _server(&_loop, muduo::net::InetAddress("0.0.0.0", port), "Server",
                  muduo::net::TcpServer::kReusePort),
          _dispatcher(std::bind(&Server::OnUnknownMessage, this,
                                std::placeholders::_1, std::placeholders::_2, std::placeholders::_3)),
          _codec(std::bind(&ProtobufDispatcher::onProtobufMessage, &_dispatcher,
                           std::placeholders::_1, std::placeholders::_2, std::placeholders::_3)) {
        _server.setConnectionCallback(std::bind(&Server::onConnection, 
            this, std::placeholders::_1));
        _server.setMessageCallback(std::bind(&Server::onMessage, this,
             std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
        _dispatcher.registerMessageCallback<bit::TranslateRequest>(
            std::bind(&Server::onTranslateRequest, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
        _dispatcher.registerMessageCallback<bit::AddRequest>(    
            std::bind(&Server::onAddRequest, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
        _dispatcher.registerMessageCallback<bit::TranslateResponse>(
            std::bind(&Server::onTranslateResponse, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
        _dispatcher.registerMessageCallback<bit::AddResponse>(
            std::bind(&Server::onAddResponse, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
    }
    void start(){
        _server.start();
        _loop.loop();
    }
    private:
    void OnUnknownMessage(const muduo::net::TcpConnectionPtr& conn, const MessagePtr& message, muduo::Timestamp receiveTime){
        LOG_ERROR << "unknown message type: " << message->GetDescriptor()->name();
        conn->shutdown();
    }
    void onConnection(const muduo::net::TcpConnectionPtr& conn){
        if(conn->connected()){
            LOG_INFO << "new connection";
        }else{
            LOG_INFO << "connection closed";
        }
    }
    void onMessage(const muduo::net::TcpConnectionPtr& conn, muduo::net::Buffer* buf, muduo::Timestamp receiveTime){
        _codec.onMessage(conn, buf, receiveTime);
    }
    void onTranslateRequest(const muduo::net::TcpConnectionPtr& conn, const TranslateRequestPtr& message, muduo::Timestamp receiveTime){
        LOG_INFO << "translate request: " << message->msg();
        bit::TranslateResponse response;
        response.set_msg("翻译结果: " + message->msg());
        _codec.send(conn, response);
    }
    void onAddRequest(const muduo::net::TcpConnectionPtr& conn, const AddRequestPtr& message, muduo::Timestamp receiveTime){
        LOG_INFO << "add request: " << message->a() << " + " << message->b() << " = " << message->a() + message->b();
        bit::AddResponse response;
        response.set_result(message->a() + message->b());
        _codec.send(conn, response);
    }
    void onTranslateResponse(const muduo::net::TcpConnectionPtr& conn, const TranslateResponsePtr& message, muduo::Timestamp receiveTime){
        LOG_INFO << "translate response: " << message->msg();
        conn->send(message->msg());
    }
    void onAddResponse(const muduo::net::TcpConnectionPtr& conn, const AddResponsePtr& message, muduo::Timestamp receiveTime){
        LOG_INFO << "add response: " << message->result();
        conn->send(std::to_string(message->result()));
    }
    
    private:
    muduo::net::EventLoop _loop;
    muduo::net::TcpServer _server;
    ProtobufDispatcher _dispatcher;
    ProtobufCodec _codec;
};