#ifndef IMAGINE_MAPREDUCE_THREADPOOL_H
#define IMAGINE_MAPREDUCE_THREADPOOL_H

#include "log_macro.h"

#include <pthread.h>
#include <list>
#include <semaphore.h>
#include <stdio.h>

namespace Imagine_MapReduce
{

template <typename T>
class ThreadPool
{
 public:
    ThreadPool(int thread_num = 10, int max_request = 10000);

    ~ThreadPool();

    void PutTask(T task);

    T GetTask();

    static void *Worker(void *data);

 private:
    int thread_num_;
    int max_request_;
    bool quit_;
    pthread_t *threads_;
    std::list<T> tasks_;
    pthread_mutex_t lock_;
    sem_t sem_;
};

template <typename T>
ThreadPool<T>::ThreadPool(int thread_num, int max_request) : thread_num_(thread_num), max_request_(max_request), quit_(false), threads_(nullptr)
{
    if (thread_num < 0 || max_request < 0) {
        throw std::exception();
    }

    threads_ = new pthread_t[thread_num];
    if (!threads_) {
        throw std::exception();
    }

    if (pthread_mutex_init(&lock_, nullptr) != 0) {
        throw std::exception();
    }

    if (sem_init(&sem_, 0, 0) != 0) {
        throw std::exception();
    }

    for (int i = 0; i < thread_num; i++) {
        IMAGINE_MAPREDUCE_LOG("create pthread %d ...", i);
        if (pthread_create(threads_ + i, nullptr, Worker, this) != 0) {
            delete[] threads_;
            throw std::exception();
        }

        if (pthread_detach(*(threads_ + i))) {
            delete[] threads_;
            throw std::exception();
        }
    }
}

template <typename T>
ThreadPool<T>::~ThreadPool()
{
    delete[] threads_;
    quit_ = true;
    pthread_mutex_destroy(&lock_);
    sem_destroy(&sem_);
}

template <typename T>
void ThreadPool<T>::PutTask(T task)
{
    if (tasks_.size() < static_cast<size_t>(max_request_)) {
        pthread_mutex_lock(&lock_);
        tasks_.push_back(task);
        pthread_mutex_unlock(&lock_);
        sem_post(&sem_);
    } else {
        // 给客户端返回一个错误码
    }
}

template <typename T>
T ThreadPool<T>::GetTask()
{
    while (!quit_) {
        sem_wait(&sem_);
        pthread_mutex_lock(&lock_);
        if (tasks_.empty()) {
            pthread_mutex_unlock(&lock_);
            continue;
        }
        T task = tasks_.front();
        tasks_.pop_front();
        pthread_mutex_unlock(&lock_);

        return task;
    }

    return nullptr;
}

template <typename T>
void *ThreadPool<T>::Worker(void *data)
{
    ThreadPool<T> *threadpool = (ThreadPool<T> *)data;
    while (!threadpool->quit_) {
        T task = threadpool->GetTask();
        if (task) {
            task->HandleEvent();
        }
    }

    return nullptr;
}

} // namespace Imagine_MapReduce

#endif