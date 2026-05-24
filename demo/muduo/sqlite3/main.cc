#include"sqlite.hpp"
#include<cassert>
int main(){
    SqliteHelper db("./test.db");
    assert(db.open());
    //2.创建表
    std::string create_table_sql="CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER NOT NULL);";
    assert(db.execute(create_table_sql, nullptr, nullptr));
    //3.插入数据
    const char* insert_sql="INSERT INTO users(name, age) VALUES('Alice', 25);";
    assert(db.execute(insert_sql, nullptr, nullptr));
        //查询数据
    const char* select_sql="SELECT * FROM users;";
    auto callback=[](void* data, int argc, char** argv, char** column_name)->int{
        for(int i=0; i<argc; ++i){
            std::cout << column_name[i] << ": " << argv[i] << " ";
        }
        std::cout << std::endl;
        return 0;
    };
    assert(db.execute(select_sql, callback, nullptr));
    //4.关闭数据库
    db.close();
    return 0;
}