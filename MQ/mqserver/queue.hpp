#ifndef __M_QUEUE_H__
#define __M_QUEUE_H__
#include "../mqcommon/logger.hpp"
#include "../mqcommon/helper.hpp"
#include "../mqcommon/msg.pb.h"
#include <iostream>
#include <unordered_map>
#include <mutex>
#include <memory>


namespace mymq {
    //1. 定义队列类
    struct MsgQueue {
        using ptr = std::shared_ptr<MsgQueue>;
        std::string name;//1. 队列名称
        bool durable;//2. 队列持久化标志
        bool exclusive;//3. 队列独占标志
        bool auto_delete;//4. 队列自动删除标志
        google::protobuf::Map<std::string, std::string> args;//5. 队列其他参数

        MsgQueue(){}
        MsgQueue(
            const std::string &qname, 
            bool qdurable, 
            bool qexclusive,
            bool qauto_delete,
            const google::protobuf::Map<std::string, std::string> &qargs):
            name(qname), durable(qdurable), exclusive(qexclusive),
            auto_delete(qauto_delete) {
            for (const auto &p : qargs) {
                args[p.first] = p.second;
            }
        }
        //解析所有队列的参数，并存储到args中
        void setArgs(const std::string &str_args) {
            std::vector<std::string> sub_args;
            StrHelper::split(str_args, "&", sub_args);
            for (auto &str : sub_args) {
                size_t pos = str.find("=");
                std::string key = str.substr(0, pos);
                std::string val = str.substr(pos + 1);
                args[key] = val;
            }
        }
        //单纯为数据库存储而设置的,先序列化一个字符串后再存储到数据库
        std::string getArgs() {
            std::string result;
            for (auto start = args.begin(); start != args.end(); ++start) {
                result += start->first + "=" + start->second + "&";
            }
            return result;
        }
    };
    
    using QueueMap = std::unordered_map<std::string, MsgQueue::ptr>;//存储所有队列的结构体，key为队列名称，value为队列指针

    //2. 定义队列数据持久化管理类--数据存储在sqlite数据库中
    class MsgQueueMapper {
        public:
            MsgQueueMapper(const std::string &dbfile):_sql_helper(dbfile) {
                std::string path = FileHelper::parentDirectory(dbfile);
                FileHelper::createDirectory(path);
                _sql_helper.open();
                createTable();
            }
            void createTable() {
                std::stringstream sql;
                sql << "create table if not exists queue_table(";
                sql << "name varchar(32) primary key, ";
                sql << "durable int, ";
                sql << "exclusive int, ";
                sql << "auto_delete int, ";
                sql << "args varchar(128));";
                assert(_sql_helper.exec(sql.str(), nullptr, nullptr));
            }
            void removeTable() {
                std::string sql = "drop table if exists queue_table;";
                _sql_helper.exec(sql, nullptr, nullptr);
            }
            bool insert(MsgQueue::ptr &queue) {
                // insert into queue_table values('queue1', true, false, false, "k1=v1&k2=v2&");
                std::stringstream sql;
                sql << "insert into queue_table values(";
                sql << "'" << queue->name << "', ";
                sql << queue->durable << ", ";
                sql << queue->exclusive << ", ";
                sql << queue->auto_delete << ", ";
                sql << "'" << queue->getArgs() << "');";
                return _sql_helper.exec(sql.str(), nullptr, nullptr);
            }
            void remove(const std::string &name) {
                // delete from queue_table where name='queue1';
                std::stringstream sql;
                sql << "delete from queue_table where name=";
                sql << "'" << name << "';";
                _sql_helper.exec(sql.str(), nullptr, nullptr);
            }
            QueueMap recovery() {
                QueueMap result;
                std::string sql = "select name, durable, exclusive, auto_delete, args from queue_table;";
                _sql_helper.exec(sql, selectCallback, &result);
                return result;
            }
        private:
            static int selectCallback(void* arg,int numcol,char** row,char** fields) {
                QueueMap *result = (QueueMap*)arg;
                MsgQueue::ptr mqp = std::make_shared<MsgQueue>();
                mqp->name = row[0];
                mqp->durable = (bool)std::stoi(row[1]);
                mqp->exclusive = (bool)std::stoi(row[2]);
                mqp->auto_delete = (bool)std::stoi(row[3]);
                if (row[4]) mqp->setArgs(row[4]);
                result->insert(std::make_pair(mqp->name, mqp));
                return 0;
            }
        private:
            SqliteHelper _sql_helper;
    };
    //3. 定义队列管理类,对外提供的操作接口 
    class MsgQueueManager {
        public:
            using ptr = std::shared_ptr<MsgQueueManager>;
            MsgQueueManager(const std::string &dbfile):_mapper(dbfile) {
                _msg_queues = _mapper.recovery();
            }
            bool declareQueue(//创建一个队列
                const std::string &qname, 
                bool qdurable, 
                bool qexclusive,
                bool qauto_delete,
                const google::protobuf::Map<std::string, std::string> &qargs) {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _msg_queues.find(qname);
                if (it != _msg_queues.end()) {
                    return true;
                }
                MsgQueue::ptr mqp = std::make_shared<MsgQueue>();
                mqp->name = qname;
                mqp->durable = qdurable;
                mqp->exclusive = qexclusive;
                mqp->auto_delete = qauto_delete;
                mqp->args = qargs;
                if (qdurable == true) {
                    bool ret = _mapper.insert(mqp);
                    if (ret == false) return false;
                }
                _msg_queues.insert(std::make_pair(qname, mqp));
                return true;
            }
            void deleteQueue(const std::string &name) {//删除一个队列
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _msg_queues.find(name);
                if (it == _msg_queues.end()) {
                    return ;
                }
                if (it->second->durable == true) _mapper.remove(name);//把数据库里的队列删除
                _msg_queues.erase(name);//把哈希表中的队列删除
            }
            MsgQueue::ptr selectQueue(const std::string &name) {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _msg_queues.find(name);
                if (it == _msg_queues.end()) {
                    return MsgQueue::ptr();
                }
                return it->second;
            }
            QueueMap allQueues() {
                std::unique_lock<std::mutex> lock(_mutex);
                return _msg_queues;
            }
            bool exists(const std::string &name) {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _msg_queues.find(name);
                if (it == _msg_queues.end()) {
                    return false;
                }
                return true;
            }
            size_t size() {
                std::unique_lock<std::mutex> lock(_mutex);
                return _msg_queues.size();
            }
            void clear() {
                _mapper.removeTable();
                _msg_queues.clear();
            }
        private:
            std::mutex _mutex;
            MsgQueueMapper _mapper;
            QueueMap _msg_queues;
    };
}
#endif