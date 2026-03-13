#ifndef _TASKSYS_H
#define _TASKSYS_H
#pragma once

#include "common/Status.hpp"
#include "itasksys.hpp"
#include <atomic>
#include <condition_variable>
#include <functional>
#include <future>
#include <mutex>
#include <queue>
#include <set>
#include <thread>

class TaskGroup {
public:
  int groupId{};
  int total_num_tasks;
  IRunnable *runnable;
  std::atomic<int> taskRemain;
  std::set<TaskID> depending;

  TaskGroup(int groupId, IRunnable *runnable, int numTotalTasks,
            const std::vector<TaskID> &deps) {
    this->groupId = groupId;
    this->runnable = runnable;
    this->total_num_tasks = numTotalTasks;
    this->taskRemain = numTotalTasks;
    this->depending = {};
    for (auto dep : deps) {
      this->depending.insert(dep);
    }
  }
  friend bool operator<(const TaskGroup &a, const TaskGroup &b) {
    return a.depending.size() > b.depending.size();
  }
};

struct RunnableTask {
public:
  TaskGroup *taskGroup;
  int id;
  RunnableTask(TaskGroup *taskGroup_, int id_)
      : id(id_), taskGroup(taskGroup_) {}
};

class TaskSystemParallelThreadPoolSleeping : public ITaskSystem {
public:
  TaskSystemParallelThreadPoolSleeping(int num_threads);

  ~TaskSystemParallelThreadPoolSleeping();

  template <typename Func, typename... Args>
  auto run(Func &&func, Args &&...args)
      -> std::future<typename std::invoke_result<Func, Args...>::type>;

private:
  void run(IRunnable *runnable, int num_total_tasks);

  TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                          const std::vector<TaskID> &deps);

  void sync();

  void func();

  std::vector<std::thread> threads;
  std::queue<RunnableTask *> taskQueue;
  std::set<TaskGroup *> taskGroupSet;
  std::priority_queue<TaskGroup *> taskGroupQueue;
  std::atomic<int> taskRemained;
  bool exitFlag;
  bool finishFlag;
  int numGroup;
  std::mutex cntMtx;
  std::mutex queueMtx;
  std::condition_variable countCond;
  std::condition_variable queueCond;
};

#endif
