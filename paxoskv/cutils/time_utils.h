
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <chrono>
#include <string>
#include <cstring>
#include <cassert>
#include "log_utils.h"

namespace cutils {


inline uint32_t get_curr_second()                                            
{                                                                            
    auto now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    return std::chrono::duration_cast< 
        std::chrono::seconds>(duration).count();
}
                                                                             
inline size_t get_curr_ms()
{
    auto now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    return std::chrono::duration_cast<
        std::chrono::milliseconds>(duration).count();
}
                                                                             
inline size_t calculate_ms(
        const std::chrono::time_point<std::chrono::system_clock>& time)
{
    auto duration = time.time_since_epoch();
    return std::chrono::duration_cast<
        std::chrono::milliseconds>(duration).count();
}


inline std::string 
format_time(std::chrono::time_point<std::chrono::system_clock> tp)
{
    auto ttp = std::chrono::system_clock::to_time_t(tp);
    
    std::string str(26, '\0');
    ctime_r(&ttp, &str[0]);
    auto str_len = std::strlen(str.data());
    assert(0 < str_len);
    // remove the added new line
    str[str_len-1] = '\0';
    str.resize(str_len);
    return str;
}


template <typename F, typename ...Args>
inline std::tuple<typename std::result_of<F(Args...)>::type, std::chrono::milliseconds>
execution(F func, Args&&... args) {
    auto start = std::chrono::system_clock::now();
    auto ret = func(std::forward<Args>(args)...);
    auto duration = std::chrono::duration_cast<
        std::chrono::milliseconds>(
                std::chrono::system_clock::now() - start);
    return std::make_tuple(ret, duration);
}


class TickTime {
public:
    template <typename ...Args>
    TickTime(const char* format, Args&&... args)
        : start_(std::chrono::system_clock::now())
    {
        msg_.resize(64, 0);
        snprintf(&msg_[0], msg_.size(), 
                format, std::forward<Args>(args)...);
        msg_.resize(std::strlen(msg_.data()));
    }

    ~TickTime()
    {
        if (true == has_print_) {
            return ;
        }
    
        print();
    }

    void print()
    {
         auto duration = 
            std::chrono::duration_cast<
                std::chrono::milliseconds>(
                        std::chrono::system_clock::now() - start_);

         if (0 < duration.count()) {
             logdebug("cost time %d = %s", 
                     static_cast<int>(duration.count()), msg_.c_str());
         }
         
         has_print_ = true;
         assert(20 > duration.count());
    }

private:
    std::chrono::time_point<std::chrono::system_clock> start_;
    std::string msg_;
    bool has_print_ = false;
};



} // namespace cutils


