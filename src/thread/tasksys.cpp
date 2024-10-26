#include "thread/tasksys.hpp"
#include "common/Status.hpp"
#include "common/ZeitgeistDB.hpp"
#include <functional>
#include <memory>
#include <mutex>
#include <utility>

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

class FunctionRunnable : public IRunnable {
public:
  FunctionRunnable(std::function<void()> func) : func_(func) {}

  void runTask(int task_id, int num_total_tasks) override { func_(); }

private:
  std::function<void()> func_;
};

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(
    int num_threads)
    : ITaskSystem(num_threads) {
  exitFlag = false;
  numGroup = 0;
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(&TaskSystemParallelThreadPoolSleeping::func, this);
  }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
  exitFlag = true;
  queueCond.notify_all();
  for (auto &thread : threads) {
    thread.join();
  }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable,
                                               int num_total_tasks) {
  runAsyncWithDeps(runnable, num_total_tasks, {});
  sync();
}

void TaskSystemParallelThreadPoolSleeping::func() {
  RunnableTask *task;
  TaskGroup *task_group;
  while (true) {
    queueCond.notify_all();
    while (true) {
      std::unique_lock<std::mutex> lock(queueMtx);
      queueCond.wait(lock, [this] { return exitFlag || !taskQueue.empty(); });
      if (exitFlag) {
        return;
      }
      if (taskQueue.empty()) {
        continue;
      }
      task = taskQueue.front();
      taskQueue.pop();
      break;
    }
    task_group = task->taskGroup;
    task_group->runnable->runTask(task->id, task_group->total_num_tasks);
    task_group->taskRemain--;
    if (task_group->taskRemain <= 0) {
      for (auto &task : taskGroupSet) {
        task_group->depending.erase(task->groupId);
      }
      countCond.notify_one();
    } else {
      queueCond.notify_all();
    }
  }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(
    IRunnable *runnable, int num_total_tasks, const std::vector<TaskID> &deps) {
  auto new_task_group =
      new TaskGroup(numGroup, runnable, num_total_tasks, deps);
  taskGroupQueue.push(new_task_group);
  taskGroupSet.insert(new_task_group);

  return numGroup++;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
  TaskGroup *task_group;
  RunnableTask *runnable_group;

  while (!taskGroupQueue.empty()) {
    task_group = taskGroupQueue.top();
    if (!task_group->depending.empty()) {
      continue;
    }
    queueMtx.lock();
    for (int i = 0; i < task_group->total_num_tasks; i++) {
      runnable_group = new RunnableTask(task_group, i);
      taskQueue.push(runnable_group);
    }
    queueMtx.unlock();
    queueCond.notify_all();
    taskGroupQueue.pop();
  }
  while (true) {
    std::unique_lock<std::mutex> lock(cntMtx);
    countCond.wait(lock, [this] { return finishFlag; });
    finishFlag = true;
    for (auto &task_group : taskGroupSet) {
      if (task_group->taskRemain > 0) {
        finishFlag = false;
        break;
      }
    }
    if (finishFlag) {
      return;
    }
  }
  return;
}

template <typename Func, typename... Args>
auto TaskSystemParallelThreadPoolSleeping::run(Func &&func, Args &&...args)
    -> std::future<typename std::invoke_result<Func, Args...>::type> {
  using return_type = typename std::result_of<Func(Args...)>::type;

  auto task = std::make_shared<std::packaged_task<return_type()>>(
      std::bind(std::forward<Func>(func), std::forward<Args>(args)...));

  std::future<return_type> result = task->get_future();

  auto wrapped_func = [task]() { (*task)(); };

  std::shared_ptr<FunctionRunnable> runnable =
      std::make_shared<FunctionRunnable>(wrapped_func);

  runAsyncWithDeps(runnable.get(), 1, {});
  sync();

  return result;
}