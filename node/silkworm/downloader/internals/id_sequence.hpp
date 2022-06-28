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

#ifndef SILKWORM_IDSEQUENCE_HPP
#define SILKWORM_IDSEQUENCE_HPP

#include "random_number.hpp"

namespace silkworm {

    class MonotonicIdSequence {
        uint64_t prefix_{0};
        uint64_t count_{0};

      public:
        MonotonicIdSequence() {
            RandomNumber random(100'000'000, 1'000'000'000);
            prefix_ = random.generate_one();
        }

        uint64_t generate_one() {
            count_++;
            if (count_ >= 10000) count_ = 0;
            return prefix_ * 10000 + count_;
        }

        bool contains(uint64_t id) {
            uint64_t prefix = id / 10000;
            return prefix_ == prefix;
        }

        uint64_t prefix() {
            return prefix_;
        }
    };

}


#endif  // SILKWORM_IDSEQUENCE_HPP
