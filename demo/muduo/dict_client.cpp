#include<muduo/net/EventLoop.h>
#include<muduo/net/TcpClient.h>
#include<muduo/net/TcpConnection.h>
#include<muduo/base/CountDownLatch.h>
#include<muduo/net/EventLoopThread.h>
#include<iostream>
#include<functional>
class TranslateClient
{
    public:
        TranslateClient(const std::string&sip,int sport):_latch(1),_client(_loopThread.startLoop(),
        muduo::net::InetAddress(sip, sport),"TranslateClient"){
            _client.setConnectionCallback(
                std::bind(&TranslateClient::onConnection,this,std::placeholders::_1));
            _client.setMessageCallback(
                std::bind(&TranslateClient::onMessage,this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3));
        }
        //连接服务器,需要等待连接建立成功后再返回
        void connect(){
            _client.connect();
            _latch.wait();
        };
        void send(const std::string& message){
            if(_conn->connected()){
                _conn->send(message);
            }
        };
        private:
        //连接状态变化时的调用函数
        void onConnection(const muduo::net::TcpConnectionPtr& conn){
          if(conn->connected()){  
                std::cout<<"连接建立成功\n";
                _latch.countDown();
                _conn=conn;
          }else{
              _conn.reset();
          }
        };
        //收到消息时的调用函数
        void onMessage(const muduo::net::TcpConnectionPtr& conn,
        muduo::net::Buffer* buf,
        muduo::Timestamp receiveTime){
            std::string str=buf->retrieveAllAsString();
            std::cout<<"收到服务器的消息:"<<str<<"\n";
        };
    private:
        muduo::CountDownLatch _latch;
        //创建一个线程进行事件循环
        muduo::net::EventLoopThread _loopThread;
        muduo::net::TcpConnectionPtr _conn;
        muduo::net::TcpClient _client;
};

int main()
{
    TranslateClient client("127.0.0.1", 8888);
    client.connect();
    while(1){
        std::string message;
        std::cin>>message;
        if(message=="exit"){
            break;
        }
        client.send(message);
    }
    return 0;
}