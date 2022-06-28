/*
Copyright 2022 The Silkworm Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#ifndef SILKWORM_EXECUTIONDURATION_H
#define SILKWORM_EXECUTIONDURATION_H

#include "stopwatch.hpp"

namespace silkworm {

    class ExecutionTime {
        StopWatch::Duration duration_{};
        StopWatch::TimePoint last_start_time_{};

      public:
        void start() { last_start_time_ = std::chrono::high_resolution_clock::now(); }
        void stop()  { duration_ += std::chrono::high_resolution_clock::now() - last_start_time_; }
        void reset() { duration_ = StopWatch::Duration::zero(); };
        StopWatch::Duration value() { return duration_; }
        std::string format() { return StopWatch::format(duration_); }
    };
}


#endif  // SILKWORM_EXECUTIONDURATION_H
