#pragma once
#include<queue>
#include<thread>
#include<memory>
#include<condition_variable>
#include <functional>
#include<iostream>
#include<vector>
class Semphore {
public:
	Semphore(int count = 0):count_(count)
	{}
	void post() {
		std::unique_lock<std::mutex> lock(mtx_);
		count_++;
		cond_.notify_all();
	}
	void wait() {
		std::unique_lock<std::mutex> lock(mtx_);
		cond_.wait(lock, [&]() {return count_ > 0; });
		count_--;
	}
private:
	std::condition_variable cond_;
	std::mutex mtx_;
	int count_;
};

class Any {
public:
	Any() = default;
	Any(const Any&) = delete;
	Any& operator=(const Any&) = delete;
	Any(Any&&) = default;
	Any& operator=(Any&&) = default;
	~Any() = default;
	template<typename T>
	Any(const T& data) :base_(std::make_unique<Derived<T>>(data))
	{}
	template<typename T>
	T cast_() {
          Derived<T>* ptr = dynamic_cast<Derived<T>*>(base_.get());
          if(ptr == nullptr) {
              throw std::bad_cast();
          }
          return ptr->getData();
      }

private:
	class Base {
	public:
		virtual ~Base() = default;
	};
	template<typename T>
	class Derived : public Base {
	private:
		T data_;
		
	public:
		T getData() const {
			return data_;
		}
		Derived(const T& data) :data_(data) {}
	};
	
private:
	std::unique_ptr<Base>base_;
};

class Task;

class Result {
public:
	Result(std::shared_ptr<Task>sp, bool isValid);
	~Result() = default;
	Result( Result&&) = default;
	void set(Any any);
	Any get();
private:
	std::shared_ptr<Task> sp_;
	Semphore sem_;
	Any any_;
	bool isValid_;
};


class Task {
public:
	Task() = default;
	virtual ~Task() = default;
	Task(const Task&) = default;
	Task& operator=(const Task&) = default;
	virtual Any run() = 0;
	void exec();
	void setResult(Result* result);
private:
	Result* result_;

};
class Thread {
public:
	using ThreadFunc = std::function<void()>;
	Thread(ThreadFunc func);
	virtual ~Thread() = default;
	Thread(const Thread&) = delete;
	Thread& operator=(const Thread&) = delete;
	void start();
private:
	ThreadFunc func_;
};




enum class PoolMode {
	MODE_FIXED,
	MODE_CACHED
};

class ThreadPool {
public:
	ThreadPool();
	~ThreadPool() = default;
	void start(int threadSize);
	int getTaskQueSize()const {
		return taskQueSize_;
	}
	void setPoolMode(PoolMode mode = PoolMode::MODE_FIXED);
	void threadFunc();
	PoolMode getPoolMode() const {
		return poolMode_;
	}
	Result submitTask(std::shared_ptr<Task>task);
private:
	std::queue<std::shared_ptr<Task>>taskQue_;
	std::vector <std::unique_ptr<Thread>>threads_;
	int taskQueSize_;
	int taskMaxSize_;
	int threadSize_;
	PoolMode poolMode_;
	std::condition_variable notFull_;
	std::condition_variable notEmpty_;
	std::mutex mtx_;

};
