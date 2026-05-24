#ifndef __M_HOST_H__
#define __M_HOST_H__
#include "exchange.hpp"
#include "queue.hpp"
#include "binding.hpp"
#include "message.hpp"


namespace mymq {

    class VirtualHost {
        public:
            using ptr = std::shared_ptr<VirtualHost>;//虚拟机类的指针

            VirtualHost(const std::string &hname, const std::string &basedir, const std::string &dbfile):
                _host_name(hname),
                _emp(std::make_shared<ExchangeManager>(dbfile)),
                _mqmp(std::make_shared<MsgQueueManager>(dbfile)),
                _bmp(std::make_shared<BindingManager>(dbfile)),
                _mmp(std::make_shared<MessageManager>(basedir)) {
                //获取到所有的队列信息，通过队列名称恢复历史消息数据
                QueueMap qm = _mqmp->allQueues();
                for (auto &q : qm) {
                    _mmp->initQueueMessage(q.first);
                }
            }

            bool declareExchange(const std::string &name,
                mq::ExchangeType type, bool durable, bool auto_delete,
                const google::protobuf::Map<std::string, std::string> &args) {
                
                return _emp->declareExchange(name, type, durable, auto_delete, args);
            }
            void deleteExchange(const std::string &name) {
                //删除交换机的时候，需要将交换机相关的绑定信息也删除掉。
                _bmp->removeExchangeBindings(name);
                return _emp->deleteExchange(name);
            }
            bool existsExchange(const std::string &name) {
                return _emp->exists(name);
            }
            Exchange::ptr selectExchange(const std::string &ename) {
                return _emp->selectExchange(ename);
            }
            //创建一个队列
            bool declareQueue(const std::string &qname, 
                bool qdurable, 
                bool qexclusive,
                bool qauto_delete,
                const google::protobuf::Map<std::string, std::string> &qargs) {
                //初始化队列的消息句柄（消息的存储管理）
                //队列的创建
                _mmp->initQueueMessage(qname);
                return _mqmp->declareQueue(qname, qdurable, qexclusive, qauto_delete, qargs);
            }
            void deleteQueue(const std::string &name) {
                //删除的时候队列相关的数据有两个：队列的消息，队列的绑定信息
                _mmp->destroyQueueMessage(name);
                _bmp->removeMsgQueueBindings(name);
                return _mqmp->deleteQueue(name);
            }
            bool existsQueue(const std::string &name) {
                return _mqmp->exists(name);
            }
            //获取所有队列
            QueueMap allQueues() {
                return _mqmp->allQueues();
            }
            //交换机和队列的绑定操作
            bool bind(const std::string &ename, const std::string &qname, const std::string &key) {
                Exchange::ptr ep = _emp->selectExchange(ename);
                if (ep.get() == nullptr) {
                    DLOG("进行队列绑定失败，交换机%s不存在！", ename.c_str());
                    return false;
                }
                MsgQueue::ptr mqp = _mqmp->selectQueue(qname);
                if (mqp.get() == nullptr) {
                    DLOG("进行队列绑定失败，队列%s不存在！", qname.c_str());
                    return false;
                }
                return _bmp->bind(ename, qname, key, ep->durable && mqp->durable);
            }
            //交换机和队列的解绑操作
            void unBind(const std::string &ename, const std::string &qname) {
                return _bmp->unBind(ename, qname);
            }
            //获取所有绑定
            MsgQueueBindingMap exchangeBindings(const std::string &ename) {
                return _bmp->getExchangeBindings(ename);
            }
            bool existsBinding(const std::string &ename, const std::string &qname) {
                return _bmp->exists(ename, qname);
            }

            //发布一条消息到队列中
            bool basicPublish(const std::string &qname, mq::BasicProperties *bp, const std::string &body) {
                MsgQueue::ptr mqp = _mqmp->selectQueue(qname);
                if (mqp.get() == nullptr) {
                    DLOG("发布消息失败，队列%s不存在！", qname.c_str());
                    return false;
                }
                return _mmp->insert(qname, bp, body, mqp->durable);
            }
            //从队列中拿出一条消息
            MessagePtr basicConsume(const std::string &qname) {
                return _mmp->front(qname);
            }
            //接受一条消息
            void basicAck(const std::string &qname, const std::string &msgid) {
                return _mmp->ack(qname, msgid);
            }  
            void clear() {
                _emp->clear();
                _mqmp->clear();
                _bmp->clear();
                _mmp->clear();
            } 
        private:
            std::string _host_name;//1.虚拟机的名称
            ExchangeManager::ptr _emp;//2.交换机的管理指针
            MsgQueueManager::ptr _mqmp;//3.队列的管理指针
            BindingManager::ptr _bmp;//4.绑定的管理指针
            MessageManager::ptr _mmp;//5.消息的管理指针
    };
}

#endif