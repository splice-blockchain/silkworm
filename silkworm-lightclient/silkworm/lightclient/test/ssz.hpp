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

#include <catch2/catch.hpp>

#include <silkworm/common/base.hpp>
#include <silkworm/common/util.hpp>
#include <silkworm/lightclient/ssz/ssz_codec.hpp>

namespace silkworm::test {

template <class T>
static bool encode_success(T& obj, std::string_view encoded_hex) {
    Bytes encoded_bytes{};
    REQUIRE(ssz::encode(obj, encoded_bytes) == EncodingResult::kOk);
    return to_hex(encoded_bytes) == encoded_hex;
}

template <class T>
static bool encode_success(T& obj, ByteView encoded_view) {
    return encode_success<T>(obj, to_hex(encoded_view));
}

template <class T>
static Bytes encode_success(T& obj) {
    Bytes encoded_bytes{};
    REQUIRE(ssz::encode(obj, encoded_bytes) == EncodingResult::kOk);
    return encoded_bytes;
}

template <class T>
static T decode_success(std::string_view encoded_hex) {
    Bytes encoded_bytes{*from_hex(encoded_hex)};
    T res{};
    REQUIRE(ssz::decode(encoded_bytes, res) == DecodingResult::kOk);
    return res;
}

template <class T>
static DecodingResult decode_failure(std::string_view encoded_hex) {
    Bytes encoded_bytes{*from_hex(encoded_hex)};
    T res{};
    return ssz::decode(encoded_bytes, res);
}

template <class T>
static DecodingResult decode_failure(ByteView encoded_view) {
    T res{};
    return ssz::decode(encoded_view, res);
}

}  // namespace silkworm::test