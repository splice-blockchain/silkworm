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

#include "ssz_codec.hpp"

#include <catch2/catch.hpp>

#include <silkworm/common/util.hpp>
#include <silkworm/lightclient/test/ssz.hpp>

namespace silkworm::ssz {

TEST_CASE("uint32_t SSZ") {
    SECTION("round-trip") {
        uint32_t a{4294967295};
        Bytes b{};
        (void)ssz::encode(a, b);
        CHECK(b == *from_hex("0xFFFFFFFF"));
        CHECK(test::decode_success<uint32_t>(to_hex(b)) == a);
    }
    SECTION("decoding error") {
        CHECK(test::decode_failure<uint32_t>("") == DecodingResult::kInputTooShort);
        CHECK(test::decode_failure<uint32_t>("00") == DecodingResult::kInputTooShort);
        CHECK(test::decode_failure<uint32_t>("0xFFFFFF") == DecodingResult::kInputTooShort);
    }
}

TEST_CASE("uint64_t SSZ") {
    SECTION("round-trip") {
        uint64_t a{18446744073709551615u};
        Bytes b{};
        (void)ssz::encode(a, b);
        CHECK(b == *from_hex("0xFFFFFFFFFFFFFFFF"));
        CHECK(test::decode_success<uint64_t>(to_hex(b)) == a);
    }
    SECTION("decoding error") {
        CHECK(test::decode_failure<uint64_t>("") == DecodingResult::kInputTooShort);
        CHECK(test::decode_failure<uint64_t>("00") == DecodingResult::kInputTooShort);
        CHECK(test::decode_failure<uint64_t>("0xFFFFFFFFFFFFFF") == DecodingResult::kInputTooShort);
    }
}

TEST_CASE("evmc::address SSZ") {
    SECTION("round-trip") {
        evmc::address a{0xFF000000000000000000000000000000000000FF_address};
        Bytes b{};
        (void)ssz::encode(a, b);
        CHECK(b == *from_hex("0xFF000000000000000000000000000000000000FF"));
        CHECK(test::decode_success<evmc::address>(to_hex(b)) == a);
    }
    SECTION("decoding error") {
        CHECK(test::decode_failure<evmc::address>("") == DecodingResult::kInputTooShort);
        CHECK(test::decode_failure<evmc::address>("00") == DecodingResult::kInputTooShort);
        CHECK(test::decode_failure<evmc::address>(
                  "0xFF000000000000000000000000000000000000") == DecodingResult::kInputTooShort);
    }
}

TEST_CASE("evmc::bytes32 SSZ") {
    SECTION("round-trip") {
        evmc::bytes32 a{0xFF000000000000000000EE00000000000000000000EE000000000000000000FF_bytes32};
        Bytes b{};
        (void)ssz::encode(a, b);
        CHECK(b == *from_hex("0xFF000000000000000000EE00000000000000000000EE000000000000000000FF"));
        CHECK(test::decode_success<evmc::bytes32>(to_hex(b)) == a);
    }
    SECTION("decoding error") {
        CHECK(test::decode_failure<evmc::bytes32>("") == DecodingResult::kInputTooShort);
        CHECK(test::decode_failure<evmc::bytes32>("00") == DecodingResult::kInputTooShort);
        CHECK(test::decode_failure<evmc::bytes32>(
                  "0xFF000000000000000000EE00000000000000000000EE000000000000000000") == DecodingResult::kInputTooShort);
    }
}

TEST_CASE("ssz::decode_dynamic_length") {
    constexpr std::size_t kMaxLength{12};

    SECTION("zero buffer") {
        std::size_t length{0};
        CHECK(ssz::decode_dynamic_length(Bytes{}, kMaxLength, length) == DecodingResult::kOk);
        CHECK(length == 0);
    }
    SECTION("buffer too short") {
        std::size_t length{0};
        CHECK(ssz::decode_dynamic_length(*from_hex("0C"), kMaxLength, length) == DecodingResult::kInputTooShort);
        CHECK(ssz::decode_dynamic_length(*from_hex("0C00"), kMaxLength, length) == DecodingResult::kInputTooShort);
        CHECK(ssz::decode_dynamic_length(*from_hex("0C0000"), kMaxLength, length) == DecodingResult::kInputTooShort);
    }
    SECTION("invalid offset") {
        std::size_t length{0};
        CHECK(ssz::decode_dynamic_length(*from_hex("05000000"), kMaxLength, length) == DecodingResult::kUnexpectedLength);
    }
    SECTION("invalid offset") {
        std::size_t length{0};
        CHECK(ssz::decode_dynamic_length(*from_hex("32000000"), kMaxLength, length) == DecodingResult::kUnexpectedLength);
    }
    SECTION("OK") {
        std::size_t length{0};
        CHECK(ssz::decode_dynamic_length(*from_hex("0C000000"), kMaxLength, length) == DecodingResult::kOk);
        CHECK(length == kMaxLength / kBytesPerLengthOffset);
    }
}

TEST_CASE("ssz::decode_dynamic") {
    const DynamicReader kEmptyReader{};
    const DynamicReader kNopReader = [](std::size_t, ByteView) { return DecodingResult::kOk; };

    SECTION("zero length") {
        CHECK(ssz::decode_dynamic(Bytes{}, 0, kEmptyReader) == DecodingResult::kOk);
    }
    SECTION("buffer too short") {
        CHECK(ssz::decode_dynamic(*from_hex("0C"), 1, kEmptyReader) == DecodingResult::kInputTooShort);
        CHECK(ssz::decode_dynamic(*from_hex("0C00"), 1, kEmptyReader) == DecodingResult::kInputTooShort);
        CHECK(ssz::decode_dynamic(*from_hex("0C0000"), 1, kEmptyReader) == DecodingResult::kInputTooShort);
    }
    SECTION("invalid end offset") {
        CHECK(ssz::decode_dynamic(*from_hex("0C00000000000000"), 1, kEmptyReader) == DecodingResult::kUnexpectedLength);
        CHECK(ssz::decode_dynamic(*from_hex("0C0000000D000000"), 2, kEmptyReader) == DecodingResult::kUnexpectedLength);
        CHECK(ssz::decode_dynamic(*from_hex("0C0000000D000000FFFFFFFF"), 2, kEmptyReader) == DecodingResult::kUnexpectedLength);
    }
    SECTION("OK") {
        CHECK(ssz::decode_dynamic(*from_hex("0C0000000C000000FFFFFFFF"), 2, kNopReader) == DecodingResult::kOk);
        CHECK(ssz::decode_dynamic(*from_hex("0C0000000D000000FFFFFFFFFF"), 2, kNopReader) == DecodingResult::kOk);
    }
}

TEST_CASE("ssz::success_or_throw") {
    CHECK_NOTHROW(success_or_throw(EncodingResult::kOk));
    CHECK_THROWS_AS(success_or_throw(EncodingResult::kTooManyElements), std::runtime_error);
}

}  // namespace silkworm::ssz