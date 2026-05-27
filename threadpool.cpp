
#include"threadpool.h"
#include <chrono>

const int THREAD_MAX_IDLE_TIME = 60;

int Thread::generatedId = 0;
void Thread::start() {
	std::thread t(func_,id_);
	t.detach();
}

Thread::Thread(ThreadFunc func) :func_(func),id_(generatedId++)
{ }



ThreadPool::ThreadPool() :taskQueSize_(0), taskMaxSize_(100), threadSize_(0), poolMode_(PoolMode::MODE_FIXED), isPoolRunning_(false), idleThreadSize_(0), threadMaxSize_(100), initThreadSize_(12)
{}

void ThreadPool::start(int threadSize) {
	for (int i = 0; i < threadSize; i++) {
		auto t = std::make_unique<Thread>(std::bind(&ThreadPool::threadFunc, this, std::placeholders::_1));
		int threadId = t->getId();
		threads_.emplace(threadId, std::move(t));
	}
	for (int i = 0; i < threadSize; i++) {
		threads_[i]->start();
		idleThreadSize_++;
		curThreadSize_++;
	}
	isPoolRunning_ = true;
}

Result ThreadPool::submitTask(std::shared_ptr<Task>task) {
	std::unique_lock<std::mutex>lock(mtx_);
	if (!notFull_.wait_for(lock, std::chrono::seconds(1), [&]() {return taskQueSize_ < taskMaxSize_; })) {
		std::cerr << "submit task timeout!" << std::endl;
		return Result(task,false);
	}
	taskQue_.emplace(task);
	taskQueSize_++;
	std::cout << "submit task success!" << std::endl;
	notEmpty_.notify_all();

	if (poolMode_ == PoolMode::MODE_CACHED && taskQueSize_ > idleThreadSize_ && curThreadSize_ < threadMaxSize_) {
		auto ptr = std::make_unique<Thread>(std::bind(&ThreadPool::threadFunc, this, std::placeholders::_1));
		int threadId = ptr->getId();
		threads_.emplace(threadId, std::move(ptr));
		threads_[threadId]->start();
		curThreadSize_++;
		idleThreadSize_++;
		std::cout << "create new thread" << std::endl;
	}
	return Result(task, true);
}

void ThreadPool::threadFunc(int threadId) {
	std::shared_ptr<Task> task;
	while (isPoolRunning_) {
		{

			/* // wait_for 内部做的事情等价于：
				  while (taskQueSize_ == 0) {                    // 1. 先检查条件：不满足就进入循环
					  std::cv_status status = notEmpty_.wait_for(lock, std::chrono::seconds(1));
					  if (status == std::cv_status::timeout) {   // 3. 超时了
						  break;                                 //    不管条件满不满足都退出
					  }
				  }
				  return taskQueSize_ > 0;                       // 5. 返回最终条件结果*/


			auto lastime = std::chrono::high_resolution_clock().now();
			std::unique_lock<std::mutex>lock(mtx_);
			if (poolMode_ == PoolMode::MODE_CACHED && !notEmpty_.wait_for(lock, std::chrono::seconds(1), [&]() { return taskQueSize_ > 0; })) {
				auto now = std::chrono::high_resolution_clock().now();
				auto dur = std::chrono::duration_cast<std::chrono::seconds>(now - lastime);
				if (dur.count() >= THREAD_MAX_IDLE_TIME && curThreadSize_ > initThreadSize_) {
					threads_.erase(threadId);
					curThreadSize_--;
					idleThreadSize_--;
					std::cout << "threadid: " << std::this_thread::get_id() << "exit! " << std::endl;
					return;

				}
				continue;
			}
			else {
				//std::unique_lock<std::mutex>lock(mtx_);
				notEmpty_.wait(lock, [&]() {return taskQueSize_ > 0; });
				task = taskQue_.front();
				std::cout << "tid :" << std::this_thread::get_id() << "获取任务成功 " << std::endl;
				taskQue_.pop();
				taskQueSize_--;
				if (taskQueSize_ == 0) {
					exitCond_.notify_one();
				}
				notFull_.notify_all();
			}
	
		}
		idleThreadSize_--;
		task->exec();
		std::cout << "tid :" << std::this_thread::get_id() << "任务完成 " << std::endl;
		idleThreadSize_++;

	}
}

ThreadPool::~ThreadPool() {
	std::unique_lock<std::mutex>lock(mtx_);
	notEmpty_.notify_all();
	exitCond_.wait(lock, [&]() {return taskQue_.size() == 0; });
	isPoolRunning_ = false;
}




void ThreadPool::setPoolMode(PoolMode mode) {
	poolMode_ = mode;
}





Result::Result(std::shared_ptr<Task>sp, bool isValid) :sp_(sp), isValid_(isValid)
{
	sp->setResult(this);
}

void Result::set(Any any) {
	any_ = std::move(any);
	sem_.post();
}

Any Result::get() {
	sem_.wait();	
	return std::move(any_);
}


void Task::exec() {
	result_->set(run());
}

void Task::setResult(Result* result) {
	result_ = result;
}