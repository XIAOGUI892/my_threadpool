#include "threadpool.h"

class MyTask :public Task {
public:
	MyTask(int a, int b) :begin_(a), end_(b) {}
	Any run() override {
		long long sum = 0;
		for (int i = begin_; i <= end_; i++) {
			sum += i;
		}
		return sum;
	}
private:
	int begin_;
	int end_;
};






int main() {
	ThreadPool pool;
	pool.start(4);
	Result result1 = pool.submitTask(std::make_shared<MyTask>(1, 100000));
	Result result2 = pool.submitTask(std::make_shared<MyTask>(100001, 200000));
	Result result3 = pool.submitTask(std::make_shared<MyTask>(200001, 300000));
	Result result4 = pool.submitTask(std::make_shared<MyTask>(300001, 400000));
	std::cout << result1.get().cast_<long long>() << std::endl;
	std::cout << result2.get().cast_<long long>() << std::endl;
	std::cout << result3.get().cast_<long long>() << std::endl;
	std::cout << result4.get().cast_<long long>() << std::endl;
}