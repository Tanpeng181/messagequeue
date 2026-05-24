#include "channel.hpp"

namespace mymq {
    //1.连接类，其中有链接提供的操作
    class Connection {
        public:
            using ptr = std::shared_ptr<Connection>;

            Connection(const VirtualHost::ptr &host, 
                const ConsumerManager::ptr &cmp, 
                const ProtobufCodecPtr &codec, 
                const muduo::net::TcpConnectionPtr &conn,
                const threadpool::ptr &pool) :
                _conn(conn),
                _codec(codec),
                _cmp(cmp),
                _host(host),
                _pool(pool),
                _channels(std::make_shared<ChannelManager>()){}
            
            //信道的创建    
            void openChannel(const openChannelRequestPtr &req) {
                //1. 判断信道ID是否重复,创建信道
                bool ret = _channels->openChannel(req->cid(), _host, _cmp, _codec, _conn, _pool);
                if (ret == false) {
                    DLOG("创建信道的时候，信道ID重复了");
                    return basicResponse(false, req->rid(), req->cid());
                }
                DLOG("%s 信道创建成功！", req->cid().c_str());
                //3. 给客户端进行回复
                return basicResponse(true, req->rid(), req->cid());
            }

            //信道的删除
            void closeChannel(const closeChannelRequestPtr &req) {
                _channels->closeChannel(req->cid());
                return basicResponse(true, req->rid(), req->cid());
            }
            //获取一个信道
            Channel::ptr getChannel(const std::string &cid) {
                return _channels->getChannel(cid);
            }

        private:
            void basicResponse(bool ok, const std::string &rid, const std::string &cid) {
                mq::basicCommonResponse resp;
                resp.set_rid(rid);
                resp.set_cid(cid);
                resp.set_ok(ok);
                _codec->send(_conn, resp);
            }
        private:
            muduo::net::TcpConnectionPtr _conn;//1.连接
            ProtobufCodecPtr _codec;//2.protobuf协议处理句柄，解决序列反序列和粘包问题
            ConsumerManager::ptr _cmp;//3.消费者管理句柄
            VirtualHost::ptr _host;//4.虚拟机管理句柄
            threadpool::ptr _pool;//5.线程池句柄
            ChannelManager::ptr _channels;//6.信道管理句柄，2.3.4.5.都是为了初始化信道的
    };

    //2.连接的管理
    class ConnectionManager {
        public:
            using ptr = std::shared_ptr<ConnectionManager>;
            ConnectionManager() {}
            //创建一个连接
            void newConnection(const VirtualHost::ptr &host, 
                const ConsumerManager::ptr &cmp, 
                const ProtobufCodecPtr &codec, 
                const muduo::net::TcpConnectionPtr &conn,
                const threadpool::ptr &pool) {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _conns.find(conn);
                if (it != _conns.end()) {
                    return ;
                }
                Connection::ptr self_conn = std::make_shared<Connection>(host, cmp, codec, conn, pool);
                _conns.insert(std::make_pair(conn, self_conn));
            }
            //删除一个连接
            void delConnection(const muduo::net::TcpConnectionPtr &conn) {
                std::unique_lock<std::mutex> lock(_mutex);
                _conns.erase(conn);
            }
            //获取一个连接
            Connection::ptr getConnection(const muduo::net::TcpConnectionPtr &conn) {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _conns.find(conn);
                if (it == _conns.end()) {
                    return Connection::ptr();
                }
                return it->second;
            }
        private:
            std::mutex _mutex;
            std::unordered_map<muduo::net::TcpConnectionPtr, Connection::ptr> _conns;
    };
}