#ifndef __M_EXCHANGE_H__
#define __M_EXCHANGE_H__
#include "../mqcommon/logger.hpp"
#include "../mqcommon/helper.hpp"
#include "../mqcommon/msg.pb.h"
#include <google/protobuf/map.h>
#include <iostream>
#include <unordered_map>
#include <mutex>
#include <memory>

namespace mymq {
    //1. 定义交换机类
    struct Exchange {
        //指向交换机的指针
        using ptr = std::shared_ptr<Exchange>;
        //1. 交换机名称
        std::string name;
        //2. 交换机类型
        mq::ExchangeType type;
        //3. 交换机持久化标志
        bool durable;
        //4. 是否自动删除标志
        bool auto_delete;
        //5. 其他参数
        google::protobuf::Map<std::string, std::string> args;

        Exchange() {}
        Exchange(
            const std::string &ename, 
            mq::ExchangeType etype, 
            bool edurable,
            bool eauto_delete,
            const google::protobuf::Map<std::string, std::string> &eargs):
            name(ename), type(etype), durable(edurable), 
            auto_delete(eauto_delete) {
            for (const auto &p : eargs) {
                args[p.first] = p.second;
            }
        }
        //args存储键值对，在存储数据库的时候，会组织一个格式字符串进行存储 key=val&key=val....
        //单纯为数据库存储而设置的
        void setArgs(const std::string &str_args) {//内部解析str_args字符串，将内容存储到成员中
            //key=val&key=val&
            std::vector<std::string> sub_args;
            StrHelper::split(str_args, "&", sub_args);
            for (auto &str : sub_args) {
                size_t pos = str.find("=");
                std::string key = str.substr(0, pos);
                std::string val = str.substr(pos + 1);
                args[key] = val;
            }
        }

        std::string getArgs() {//将args中的内容进行序列化后，返回一个字符串
            std::string result;
            for (auto start = args.begin(); start != args.end(); ++start) {
                result += start->first + "=" + start->second + "&";
            }
            return result;
        }
    };
    using ExchangeMap = std::unordered_map<std::string, Exchange::ptr>;//存储所有交换机的结构体，key为交换机名称，value为交换机指针
    //2. 定义交换机数据持久化管理类--数据存储在sqlite数据库中
    class ExchangeMapper {
        public:
            ExchangeMapper(const std::string &dbfile):_sql_helper(dbfile) {
                std::string path = FileHelper::parentDirectory(dbfile);
                FileHelper::createDirectory(path);
                assert(_sql_helper.open());
                createTable();
            }
            void createTable() {
                #define CREATE_TABLE "create table if not exists exchange_table(\
                    name varchar(32) primary key, \
                    type int, \
                    durable int, \
                    auto_delete int, \
                    args varchar(128));"

                bool ret = _sql_helper.exec(CREATE_TABLE, nullptr, nullptr);
                if (ret == false) {
                    DLOG("创建交换机数据库表失败！！");
                    abort();//直接异常退出程序
                }
            }
            void removeTable() {
                #define DROP_TABLE "drop table if exists exchange_table;"
                bool ret = _sql_helper.exec(DROP_TABLE, nullptr, nullptr);
                if (ret == false) {
                    DLOG("删除交换机数据库表失败！！");
                    abort();//直接异常退出程序
                }
            }
            bool insert(Exchange::ptr &exp) {
                std::stringstream ss;
                ss << "insert into exchange_table values(";
                ss << "'" << exp->name << "', ";
                ss << exp->type << ", ";
                ss << exp->durable << ", ";
                ss << exp->auto_delete << ", ";
                ss << "'" << exp->getArgs() << "');";
                return _sql_helper.exec(ss.str(), nullptr, nullptr);
            }
            void remove(const std::string &name) {
                std::stringstream ss;
                ss << "delete from exchange_table where name=";
                ss << "'" << name << "';";
                _sql_helper.exec(ss.str(), nullptr, nullptr);
            }

            ExchangeMap recovery() {//读取所有交换机的数据,并存储到ExchangeMap中
                ExchangeMap result;
                std::string sql = "select name, type, durable, auto_delete, args from exchange_table;";
                _sql_helper.exec(sql, selectCallback, &result);
                return result;
            }
        private:
            static int selectCallback(void* arg,int numcol,char** row,char** fields) {//把读取的数据存储到ExchangeMap中
                ExchangeMap *result = (ExchangeMap*)arg;
                auto exp = std::make_shared<Exchange>();
                exp->name = row[0];
                exp->type = (mq::ExchangeType)std::stoi(row[1]);
                exp->durable = (bool)std::stoi(row[2]);
                exp->auto_delete = (bool)std::stoi(row[3]);
                if (row[4]) exp->setArgs(row[4]);
                result->insert(std::make_pair(exp->name, exp));
                return 0;
            }
        private:
            SqliteHelper _sql_helper;
    };
    //3. 定义交换机数据内存管理类，对外提供的接口给程序员使用 
    class ExchangeManager {
        public:
            using ptr = std::shared_ptr<ExchangeManager>;
            ExchangeManager(const std::string &dbfile) : _mapper(dbfile){
                _exchanges = _mapper.recovery();
            }
            //声明一个交换机
            bool declareExchange(
                const std::string &name,
                mq::ExchangeType type, bool durable, bool auto_delete,
                const google::protobuf::Map<std::string, std::string> &args) {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _exchanges.find(name);
                if (it != _exchanges.end()) {
                    //如果交换机已经存在，那就直接返回，不需要重复新增。
                    return true;
                }
                auto exp = std::make_shared<Exchange>(name, type, durable, auto_delete, args);
                if (durable == true) {
                    bool ret = _mapper.insert(exp);
                    if (ret == false) return false;
                }
                _exchanges.insert(std::make_pair(name, exp));
                return true;
            }
            //删除交换机
            void deleteExchange(const std::string &name) {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _exchanges.find(name);
                if (it == _exchanges.end()) {
                    return;
                }
                if(it->second->durable == true) _mapper.remove(name);
                _exchanges.erase(name);
            }
            //获取指定交换机对象
            Exchange::ptr selectExchange(const std::string &name) {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _exchanges.find(name);
                if (it == _exchanges.end()) {
                    return Exchange::ptr();
                }
                return it->second;
            }
            //判断交换机是否存在
            bool exists(const std::string &name) {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _exchanges.find(name);
                if (it == _exchanges.end()) {
                    return false;
                }
                return true;
            }
            size_t size() {
                std::unique_lock<std::mutex> lock(_mutex);
                return _exchanges.size();
            }
            //清理所有交换机数据
            void clear() {
                std::unique_lock<std::mutex> lock(_mutex);
                _mapper.removeTable();
                _exchanges.clear();
            }
        private:
            std::mutex _mutex;
            ExchangeMapper _mapper;
            ExchangeMap _exchanges;
    };
}
#endif