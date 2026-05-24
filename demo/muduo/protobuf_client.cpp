
#include "muduo/proto/dispatcher.h"
#include "muduo/proto/codec.h"
#include "request.pb.h"
#include "muduo/net/EventLoop.h"
#include "muduo/net/TcpClient.h"
#include "muduo/net/TcpConnection.h"
#include "muduo/net/InetAddress.h"
#include "muduo/net/EventLoopThread.h"
#include "muduo/base/CountDownLatch.h"
#include <muduo/base/Logging.h>
#include <google/protobuf/message.h>
class client{
    public:
    typedef std::shared_ptr<google::protobuf::Message> MessagePtr;
    typedef std::shared_ptr<bit::TranslateRequest> TranslateRequestPtr;
    typedef std::shared_ptr<bit::TranslateResponse> TranslateResponsePtr;
    typedef std::shared_ptr<bit::AddRequest> AddRequestPtr;
    typedef std::shared_ptr<bit::AddResponse> AddResponsePtr;
    client(const std::string& sip, int sport)
        : _latch(1),
          _client(_loopThread.startLoop(), muduo::net::InetAddress(sip, sport), "Client"),
          _dispatcher(std::bind(&client::onUnknownMessage, this,
                                std::placeholders::_1, std::placeholders::_2, std::placeholders::_3)),
          _codec(std::bind(&ProtobufDispatcher::onProtobufMessage, &_dispatcher,
                           std::placeholders::_1, std::placeholders::_2, std::placeholders::_3)) {
        _client.setConnectionCallback(
            std::bind(&client::onConnection, this, std::placeholders::_1));
        _client.setMessageCallback(
            std::bind(&client::onMessage, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
        _dispatcher.registerMessageCallback<bit::TranslateRequest>(
            std::bind(&client::onTranslateRequest, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
        _dispatcher.registerMessageCallback<bit::AddRequest>(
            std::bind(&client::onAddRequest, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
        _dispatcher.registerMessageCallback<bit::TranslateResponse>(
            std::bind(&client::onTranslateResponse, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
        _dispatcher.registerMessageCallback<bit::AddResponse>(
            std::bind(&client::onAddResponse, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
    }
    void connect(){
        _client.connect();
        _latch.wait();
    }
    void send(const std::string& message){
        if(_conn->connected()){
            _conn->send(message);
        }
    }
    void Translate(const std::string& message){
        bit::TranslateRequest request;
        request.set_msg(message);
        _codec.send(_conn, request);
    }
    void Add(int a, int b){
        bit::AddRequest request;
        request.set_a(a);
        request.set_b(b);
        _codec.send(_conn, request);
    }
    void TranslateResponse(const std::string& message){
        bit::TranslateResponse response;
        response.set_msg(message);
        _codec.send(_conn, response);
    }
    void AddResponse(int result){
        bit::AddResponse response;
        response.set_result(result);
        _codec.send(_conn, response);
    }
    private:
    void onConnection(const muduo::net::TcpConnectionPtr& conn){
        if(conn->connected()){
            LOG_INFO << "new connection";
        }else{
            LOG_INFO << "connection closed";
        }
    }
    void onUnknownMessage(const muduo::net::TcpConnectionPtr& conn, const MessagePtr& message, muduo::Timestamp receiveTime){
        LOG_ERROR << "unknown message type: " << message->GetDescriptor()->name();
        conn->shutdown();
    }
    void onTranslateRequest(const muduo::net::TcpConnectionPtr& conn, const TranslateRequestPtr& message, muduo::Timestamp receiveTime){
        LOG_INFO << "translate request: " << message->msg();
        bit::TranslateResponse response;
        response.set_msg("翻译结果: " + message->msg());
        _codec.send(conn, response);
    }
    void onAddRequest(const muduo::net::TcpConnectionPtr& conn, const AddRequestPtr& message, muduo::Timestamp receiveTime){
        LOG_INFO << "add request: " << message->a() << " + " << message->b() << " = " << message->a() + message->b();
    }
    void onMessage(const muduo::net::TcpConnectionPtr& conn, muduo::net::Buffer* buf, muduo::Timestamp receiveTime){
        _codec.onMessage(conn, buf, receiveTime);
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
    muduo::net::EventLoopThread _loopThread;
    muduo::net::TcpClient _client;
    muduo::CountDownLatch _latch;
    muduo::net::TcpConnectionPtr _conn;
    ProtobufDispatcher _dispatcher;
    ProtobufCodec _codec;
};

