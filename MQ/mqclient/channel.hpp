#ifndef __M_CHANNEL_H__
#define __M_CHANNEL_H__
#include "muduo/net/TcpConnection.h"
#include "muduo/proto/codec.h"
#include "muduo/proto/dispatcher.h"
#include "../mqcommon/logger.hpp"
#include "../mqcommon/helper.hpp"
#include "../mqcommon/msg.pb.h"
#include "../mqcommon/proto.pb.h"
#include "consumer.hpp"
#include <iostream>
#include <mutex>
#include <condition_variable>
#include <unordered_map>

namespace mymq {
    typedef std::shared_ptr<google::protobuf::Message> MessagePtr;
    using ProtobufCodecPtr = std::shared_ptr<ProtobufCodec>;//协议处理对象的指针
    using basicConsumeResponsePtr = std::shared_ptr<mq::basicConsumeResponse>;//管理消息推送的指针
    using basicCommonResponsePtr = std::shared_ptr<mq::basicCommonResponse>;//管理通用响应的指针
    class Channel {
        public:
            using ptr = std::shared_ptr<Channel>;
            Channel(const muduo::net::TcpConnectionPtr& conn, const ProtobufCodecPtr& codec):
                _cid(UUIDHelper::uuid()), _conn(conn), _codec(codec) {}
            ~Channel() { basicCancel(); }
            //获取信道id
            std::string cid() { return _cid; }
            //打开信道
            bool openChannel() {
                std::string rid = UUIDHelper::uuid();
                mq::openChannelRequest req; 
                req.set_rid(rid);
                req.set_cid(_cid);
                _codec->send(_conn, req);
                basicCommonResponsePtr resp = waitResponse(rid);
                return resp->ok();
            }
            //关闭信道
            void closeChannel() {
                std::string rid = UUIDHelper::uuid();
                mq::closeChannelRequest req; 
                req.set_rid(rid);
                req.set_cid(_cid);
                _codec->send(_conn, req);
                waitResponse(rid);
                return ;
            }
            //创建交换机
            bool declareExchange(
                const std::string &name,
                mq::ExchangeType type, 
                bool durable, 
                bool auto_delete,
                google::protobuf::Map<std::string, std::string> &args) {
                //构造一个声明虚拟机的请求对象，
                std::string rid = UUIDHelper::uuid();
                mq::declareExchangeRequest req;
                req.set_rid(rid);
                req.set_cid(_cid);
                req.set_exchange_name(name);
                req.set_exchange_type(type);
                req.set_durable(durable);
                req.set_auto_delete(auto_delete);
                req.mutable_args()->swap(args);
                //然后向服务器发送请求
                _codec->send(_conn, req);
                //等待服务器的响应
                basicCommonResponsePtr resp = waitResponse(rid);
                //返回
                return resp->ok();
            }
            //删除交换机
            void deleteExchange(const std::string &name) {
                std::string rid = UUIDHelper::uuid();
                mq::deleteExchangeRequest req;
                req.set_rid(rid);
                req.set_cid(_cid);
                req.set_exchange_name(name);
                _codec->send(_conn, req);
                //等待服务器的响应
                waitResponse(rid);
                return ;
            }
            //创建队列
            bool declareQueue(
                const std::string &qname, 
                bool qdurable, 
                bool qexclusive,
                bool qauto_delete,
                google::protobuf::Map<std::string, std::string> &qargs) {
                std::string rid = UUIDHelper::uuid();
                mq::declareQueueRequest req;
                req.set_rid(rid);
                req.set_cid(_cid);
                req.set_queue_name(qname);
                req.set_durable(qdurable);
                req.set_auto_delete(qauto_delete);
                req.set_exclusive(qexclusive);
                req.mutable_args()->swap(qargs);
                _codec->send(_conn, req);
                basicCommonResponsePtr resp = waitResponse(rid);
                return resp->ok();
            }
            //删除队列
            void deleteQueue(const std::string &qname) {
                std::string rid = UUIDHelper::uuid();
                mq::deleteQueueRequest req;
                req.set_rid(rid);
                req.set_cid(_cid);
                req.set_queue_name(qname);
                _codec->send(_conn, req);
                waitResponse(rid);
                return ;
            }
            //绑定
            bool queueBind(
                const std::string &ename, 
                const std::string &qname, 
                const std::string &key) {
                std::string rid = UUIDHelper::uuid();
                mq::queueBindRequest req;
                req.set_rid(rid);
                req.set_cid(_cid);
                req.set_exchange_name(ename);
                req.set_queue_name(qname);
                req.set_binding_key(key);
                _codec->send(_conn, req);
                basicCommonResponsePtr resp = waitResponse(rid);
                return resp->ok();
            }
            //解除绑定
            void queueUnBind(const std::string &ename, const std::string &qname) {
                std::string rid = UUIDHelper::uuid();
                mq::queueUnBindRequest req;
                req.set_rid(rid);
                req.set_cid(_cid);
                req.set_exchange_name(ename);
                req.set_queue_name(qname);
                _codec->send(_conn, req);
                waitResponse(rid);
                return ;
            }
            //消息的发布
            void basicPublish(
                const std::string &ename,//发给哪个交换机
                const mq::BasicProperties *bp,//消息属性
                const std::string &body) {
                std::string rid = UUIDHelper::uuid();
                mq::basicPublishRequest req;
                req.set_rid(rid);
                req.set_cid(_cid);
                req.set_body(body);
                req.set_exchange_name(ename);
                if (bp != nullptr) {
                    req.mutable_properties()->set_id(bp->id());
                    req.mutable_properties()->set_delivery_mode(bp->delivery_mode());
                    req.mutable_properties()->set_routing_key(bp->routing_key());
                }
                _codec->send(_conn, req);
                waitResponse(rid);
                return ;
            }
            //消息的确定
            void basicAck(const std::string &msgid) {
                if (_consumer.get() == nullptr) {
                    DLOG("消息确认时，找不到消费者信息！");
                    return ;
                }
                std::string rid = UUIDHelper::uuid();
                mq::basicAckRequest req;
                req.set_rid(rid);
                req.set_cid(_cid);
                req.set_queue_name(_consumer->qname);
                req.set_message_id(msgid);
                _codec->send(_conn, req);
                waitResponse(rid);
                return;
            }
            //取消订阅
            void basicCancel() {
                if (_consumer.get() == nullptr) {
                    return ;
                }
                std::string rid = UUIDHelper::uuid();
                mq::basicCancelRequest req;
                req.set_rid(rid);
                req.set_cid(_cid);
                req.set_queue_name(_consumer->qname);
                req.set_consumer_tag(_consumer->tag);
                _codec->send(_conn, req);
                waitResponse(rid);
                _consumer.reset();
                return;
            }
            //添加订阅
            bool basicConsume(
                const std::string &consumer_tag,
                const std::string &queue_name,
                bool auto_ack,
                const ConsumerCallback &cb) {
                if (_consumer.get() != nullptr) {
                    DLOG("当前信道已订阅其他队列消息！");
                    return false;
                }
                std::string rid = UUIDHelper::uuid();
                mq::basicConsumeRequest req;
                req.set_rid(rid);
                req.set_cid(_cid);
                req.set_queue_name(queue_name);
                req.set_consumer_tag(consumer_tag);
                req.set_auto_ack(auto_ack);
                _codec->send(_conn, req);
                basicCommonResponsePtr resp =  waitResponse(rid);
                if (resp->ok() == false) {
                    DLOG("添加订阅失败！");
                    return false;
                }
                _consumer = std::make_shared<Consumer>(consumer_tag, queue_name, auto_ack, cb);
                return true;
            }
        public:   
            //连接收到基础响应后，向hash_map中添加响应
            void putBasicResponse(const basicCommonResponsePtr& resp) {
                std::unique_lock<std::mutex> lock(_mutex);
                _basic_resp.insert(std::make_pair(resp->rid(), resp));
                _cv.notify_all();
            }
            
            //连接收到消息推送后，需要通过信道找到对应的消费者对象，通过调用消费者对象中的回调函数进行消息处理
            void consume(const basicConsumeResponsePtr& resp) {
                if (_consumer.get() == nullptr) {
                    DLOG("消息处理时，未找到订阅者信息！");
                    return;
                }
                if (_consumer->tag != resp->consumer_tag()) {
                    DLOG("收到的推送消息中的消费者标识，与当前信道消费者标识不一致！");
                    return ;
                }
                _consumer->callback(resp->consumer_tag(), resp->mutable_properties(), resp->body());
            }
        private:

            //等待响应
            basicCommonResponsePtr waitResponse(const std::string &rid) {
                std::unique_lock<std::mutex> lock(_mutex);
                _cv.wait(lock, [&rid, this](){
                    return _basic_resp.find(rid) != _basic_resp.end();
                });
                //while(condition()) _cv.wait();
                basicCommonResponsePtr basic_resp = _basic_resp[rid];
                _basic_resp.erase(rid);
                return basic_resp;
            }
        private:
            std::string _cid;//1.信道id
            muduo::net::TcpConnectionPtr _conn;//2.信道关联的连接
            ProtobufCodecPtr _codec;//3.协议处理，解决序列化反序列化和粘包问题
            Consumer::ptr _consumer;//4.信道关联的消费者

            std::mutex _mutex;//5.互斥锁
            std::condition_variable _cv;//6.条件变量
            std::unordered_map<std::string, basicCommonResponsePtr> _basic_resp;//7.管理请求id和响应的映射
            //5.6.7.是为了维持操作的正常返回
    };

    //2.信道的管理
    class ChannelManager {
        public:
            using ptr = std::shared_ptr<ChannelManager>;

            ChannelManager(){}
            //创建一个信道
            Channel::ptr create(const muduo::net::TcpConnectionPtr &conn, 
                const ProtobufCodecPtr &codec) {
                std::unique_lock<std::mutex> lock(_mutex);
                auto channel = std::make_shared<Channel>(conn, codec);
                _channels.insert(std::make_pair(channel->cid(), channel));
                return channel;
            }
            //删除一个信道
            void remove(const std::string &cid) {
                std::unique_lock<std::mutex> lock(_mutex);
                _channels.erase(cid);
            }
            //获取一个信道
            Channel::ptr get(const std::string &cid) {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _channels.find(cid);
                if (it == _channels.end()) {
                    return Channel::ptr();
                }
                return it->second;
            }
        private:
            std::mutex _mutex;
            std::unordered_map<std::string, Channel::ptr> _channels;//管理所有的信道
    };
}


#endif