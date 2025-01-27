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

#pragma once

#if defined(__wasm__)

#include <unordered_map>
#include <unordered_set>

#else

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

#endif

namespace silkworm {

/*
Alias templates to fast hash maps and sets, such as Abseil "Swiss tables"

The following aliases are defined:

FlatHashMap – a hash map that might not have pointer stability.
FlatHashSet – a hash set that might not have pointer stability.

See https://abseil.io/docs/cpp/guides/container#hash-tables
and https://abseil.io/docs/cpp/guides/container#fn:pointer-stability
*/

#if defined(__wasm__)

// Abseil is not compatible with Wasm due to its mutli-threading features,
// at least not under CMake, but see
// https://github.com/abseil/abseil-cpp/pull/721

template <class K, class V>
using FlatHashMap = std::unordered_map<K, V>;

template <class T>
using FlatHashSet = std::unordered_set<T>;

#else

template <class K, class V>
using FlatHashMap = absl::flat_hash_map<K, V>;

template <class T>
using FlatHashSet = absl::flat_hash_set<T>;

#endif

}  // namespace silkworm
