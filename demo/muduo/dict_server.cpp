#include <muduo/net/EventLoop.h>
#include <muduo/net/InetAddress.h>
#include <muduo/net/TcpServer.h>
#include<muduo/net/TcpConnection.h>
#include<iostream>
class TrasnlateServer
{   public:
        TrasnlateServer(int port):
        _server(&_baseloop, muduo::net::InetAddress('0.0.0.0', port),
         "TrasnlateServer",muduo::net::TcpServer::kReusePort){
            _server.setConnectionCallback(
                //绑定成员函数onConnection
                std::bind(&TrasnlateServer::onConnection,this,std::placeholders::_1));
            _server.setMessageCallback(
                //绑定成员函数OnMessage
                std::bind(&TrasnlateServer::OnMessage,this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3));
         }

        void start(){
            _server.start();//启动线程
            _baseloop.loop();//事件循环的本体
        }
        
    private:
    //1.连接状态变化时的调用函数
    void onConnection(const muduo::net::TcpConnectionPtr& conn)
    {
        if(conn->connected()==true){
            std::cout<<"新连接建立成功\n";
        }else{
            std::cout<<"连接断开\n";
        }
    }
    //2.收到消息时的调用函数
    void OnMessage(const muduo::net::TcpConnectionPtr& conn,
        muduo::net::Buffer* buf,
        muduo::Timestamp receiveTime)
    {
        //1.从buf中把请求的数据取出来
        std::string str=buf->retrieveAllAsString();
        //2.对请求的数据进行处理
        std::string result="翻译结果:"+str;
        //3.把处理的结果发送给客户端
        conn->send(result);
    }
    private:
    muduo::net::EventLoop _baseloop;
    //server对象,主要用于设置回调函数，告诉服务器收到什么请求该如何处理
    muduo::net::TcpServer _server;
};
int main()
{
    TrasnlateServer server(8888);
    server.start();
    return 0;
}