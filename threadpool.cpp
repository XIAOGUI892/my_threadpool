
#include"threadpool.h"

void Thread::start() {
	std::thread t(func_);
	t.detach();
}

Thread::Thread(ThreadFunc func) :func_(func)
{ }



ThreadPool::ThreadPool() :taskQueSize_(0), taskMaxSize_(100), threadSize_(0), poolMode_(PoolMode::MODE_FIXED)
{}
void ThreadPool::start(int threadSize) {
	for (int i = 0; i < threadSize; i++) {
		auto t = std::make_unique<Thread>(std::bind(&ThreadPool::threadFunc, this));
		threads_.emplace_back(std::move(t));
	}
	for (int i = 0; i < threadSize; i++) {
		threads_[i]->start();
	}
}

Result ThreadPool::submitTask(std::shared_ptr<Task>task) {
	std::unique_lock<std::mutex>lock(mtx_);
	if (!notFull_.wait_for(lock, std::chrono::seconds(1), [&]() {return taskQueSize_ < taskMaxSize_; })) {
		std::cerr << "submit task timeout!" << std::endl;
		return Result(task,false);
	}
	taskQue_.emplace(task);
	taskQueSize_++;
	notEmpty_.notify_all();
	return Result(task,true);

}

void ThreadPool::threadFunc() {
	std::shared_ptr<Task> task;
	for (;;) {
		{
			std::unique_lock<std::mutex>lock(mtx_);
			notEmpty_.wait(lock, [&]() {return taskQueSize_ > 0; });
			task = taskQue_.front();
			taskQue_.pop();
			taskQueSize_--;
			notFull_.notify_all();
		}
		if (task) {
			std::cout << "threadid: " << std::this_thread::get_id() << "begin! " << std::endl;
			task->exec();	
			std::cout << "threadid: " << std::this_thread::get_id() << "exit! " << std::endl;
		}
	}
}




Result::Result (std::shared_ptr<Task>sp, bool isValid) :sp_(sp), isValid_(isValid) {
	sp_->setResult(this);
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