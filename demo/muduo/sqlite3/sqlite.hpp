#include<iostream>
#include<string>
#include<vector>
#include<sqlite3.h>
class SqliteHelper{
    public:
    typedef int(*Callback)(void*, int, char**, char**);
    SqliteHelper(const std::string& db_name):
        _dbfile(db_name), _handler(nullptr) {}

    bool open(int safe_leve=SQLITE_OPEN_NOFOLLOW){
        int ret=sqlite3_open_v2(_dbfile.c_str(), &_handler, SQLITE_OPEN_READONLY|SQLITE_OPEN_CREATE|safe_leve, nullptr);
        if(ret != SQLITE_OK){
            std::cerr << "Failed to open database: " << sqlite3_errmsg(_handler) << std::endl;
            return false;
        }
        return true;
    }    
   bool execute(const std::string& sql, Callback callback, void* data){
        char* err_msg=nullptr;
        int ret=sqlite3_exec(_handler, sql.c_str(), callback, data, &err_msg);
        if(ret != SQLITE_OK){
            std::cerr << "Failed to execute SQL: " << err_msg << std::endl;
            sqlite3_free(err_msg);
            return false;
        }
        return true;
    }
    void close(){
        if(_handler){
            sqlite3_close(_handler);
            _handler=nullptr;
        }
    }
    
    private:
    std::string _dbfile;
    sqlite3* _handler;
};