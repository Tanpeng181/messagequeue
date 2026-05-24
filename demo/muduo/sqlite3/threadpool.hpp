#include<iostream>
#include<thread>
#include<mutex>
#include<future>
#include<memory>
#include<condition_variable>
#include<vector>

class ThreadPool {
public:
    using Functor=std::function<void()>;
    ThreadPool(size_t num_threads=5) :_stop(false){
        for(int i=0; i<num_threads; ++i){
            _threads.emplace_back(&ThreadPool::entry, this);
        }
    }
    void stop(){
        _stop=true;
        _cv.notify_all();
        for(auto& thread : _threads){
            if(thread.joinable()){
                thread.join();
            }
        }
    }
    ~ThreadPool() {}
    //push传入的第一个参数是用户要执行的函数，后面是这个函数的参数
    //push函数内部，会将这个传入的函数封装成一个异步任务（packaged_task），抛入到任务池中，由线程取出并执行
    template<typename F, typename... Args>
    auto push(F &&func,Args&& ...args)->std::future<decltype(func(args...))>{
        //1.将传入的函数封装成packaged_task任务
        using return_type=decltype(func(args...));
        auto tmp_func=std::bind(std::forward<F>(func), std::forward<Args>(args)...);
        std::packaged_task=std::make_shared<std::packaged_task<return_type()>>(tmp_func);
        std::future<return_type> future=task->get_future();
        //2.构造一个lambda函数，这个lambda函数的作用是调用packaged_task任务，并将结果保存到future对象中
        {
            std::unique_lock<std::mutex> lock(_mutex);
            //3.将这个lambda函数抛入到任务池中，由线程取出并执行
            _taskpool.emplace_back([task](){
                (*task)();
             });
             _cv.notify_one();
        }
        return future;
        
       
    }

    void stop() {}
private:
    //线程入口函数，线程池中的线程会不断地从任务池中取出任务并执行，直到线程池被停止
    void entry(){
        while(_stop==false){
            std::vector<Functor> tmp_taskpool;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                _cv.wait(lock, [this](){ return !_taskpool.empty() || _stop; });
                if(_stop && _taskpool.empty()){
                    return;
                }
                tmp_taskpool.swap(_taskpool);
                
            }
           for(auto& task : tmp_taskpool){
                task();
        }
    }
    }
private:
    std::mutex _mutex;
    std::vector<std::thread> _threads;
    std::vector<Functor> _taskpool;
    std::condition_variable _cv;
    std::atomic<bool> _stop;
};