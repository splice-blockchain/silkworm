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

#include "types.hpp"

#include <catch2/catch.hpp>

#include <silkworm/core/common/util.hpp>
#include <silkworm/lightclient/test/ssz.hpp>

namespace silkworm::cl {

TEST_CASE("Eth1Data SSZ") {
    SECTION("round-trip") {
        Eth1Data a{
            0xFF000000000000000000EE00000000000000000000EE000000000000000000FF_bytes32,
            31,
            0xFF000000000000000000EE00000000000000000000EE000000000000000000FF_bytes32
        };
        Bytes b{};
        CHECK(ssz::encode(a, b));
        CHECK(b.size() == Eth1Data::kSize);
        CHECK(b == *from_hex(
                       "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
                       "1F00000000000000"
                       "FF000000000000000000EE00000000000000000000EE000000000000000000FF"));
        CHECK(test::decode_success<Eth1Data>(to_hex(b)) == a);
    }
    SECTION("decoding error") {
        CHECK(test::decode_failure<Eth1Data>("") == DecodingError::kUnexpectedLength);
        CHECK(test::decode_failure<Eth1Data>("00") == DecodingError::kUnexpectedLength);
        CHECK(test::decode_failure<Eth1Data>("FF000000000000000000EE00000000000000000000EE000000000000000000FF"
                                             "1F00000000000000"
                                             "FF000000000000000000EE00000000000000000000EE000000000000000000")
              == DecodingError::kUnexpectedLength);
    }
}

TEST_CASE("Checkpoint SSZ") {
    SECTION("round-trip") {
        Checkpoint a{
            21,
            0xFF000000000000000000EE00000000000000000000EE000000000000000000FF_bytes32
        };
        Bytes b{};
        CHECK(ssz::encode(a, b));
        CHECK(b.size() == Checkpoint::kSize);
        CHECK(b == *from_hex(
                       "1500000000000000"
                       "FF000000000000000000EE00000000000000000000EE000000000000000000FF"));
        CHECK(test::decode_success<Checkpoint>(to_hex(b)) == a);
    }
    SECTION("decoding error") {
        CHECK(test::decode_failure<Checkpoint>("") == DecodingError::kUnexpectedLength);
        CHECK(test::decode_failure<Checkpoint>("00") == DecodingError::kUnexpectedLength);
        CHECK(test::decode_failure<Checkpoint>("1F00000000000000"
                                             "FF000000000000000000EE00000000000000000000EE000000000000000000")
              == DecodingError::kUnexpectedLength);
    }
}

TEST_CASE("AttestationData SSZ") {
    SECTION("round-trip") {
        AttestationData a{
            120,
            6,
            0xFF000000000000000000EE00000000000000000000EE000000000000000000FF_bytes32,
            std::make_unique<Checkpoint>(Checkpoint{
                21,
                0xFF000000000000000000EE00000000000000000000EE000000000000000000FF_bytes32
            }),
            std::make_unique<Checkpoint>(Checkpoint{
                21,
                0xFF000000000000000000EE00000000000000000000EE000000000000000000FF_bytes32
            }),
        };
        Bytes b{};
        CHECK(ssz::encode(a, b));
        CHECK(b.size() == AttestationData::kSize);
        CHECK(b == *from_hex(
                       "7800000000000000"
                       "0600000000000000"
                       "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
                       "1500000000000000"
                       "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
                       "1500000000000000"
                       "FF000000000000000000EE00000000000000000000EE000000000000000000FF"));
        CHECK(test::decode_success<AttestationData>(to_hex(b)) == a);
    }
    SECTION("round-trip w/ empty source and target") {
        AttestationData a{
            120,
            6,
            0xFF000000000000000000EE00000000000000000000EE000000000000000000FF_bytes32,
            {},
            {},
        };
        Bytes b{};
        CHECK(ssz::encode(a, b));
        CHECK(b.size() == AttestationData::kSize);
        CHECK(b == *from_hex(
                       "7800000000000000"
                       "0600000000000000"
                       "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
                       "0000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"));
        CHECK(test::decode_success<AttestationData>(to_hex(b)) == a);
    }
    SECTION("decoding error") {
        CHECK(test::decode_failure<AttestationData>("") == DecodingError::kUnexpectedLength);
        CHECK(test::decode_failure<AttestationData>("00") == DecodingError::kUnexpectedLength);
        CHECK(test::decode_failure<AttestationData>("7400000000000000"
                                                    "0600000000000000"
                                                    "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
                                                    "1F00000000000000"
                                                    "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
                                                    "1F00000000000000"
                                                    "FF000000000000000000EE00000000000000000000EE000000000000000000")
              == DecodingError::kUnexpectedLength);
    }
}

TEST_CASE("BeaconBlockHeader SSZ") {
    SECTION("round-trip") {
        BeaconBlockHeader a{
            21,
            120,
            0xFF000000000000000000EE00000000000000000000EE000000000000000000FF_bytes32,
            0xFF000000000000000000EE00000000000000000000EE000000000000000000FF_bytes32,
            0xFF000000000000000000EE00000000000000000000EE000000000000000000FF_bytes32
        };
        Bytes b{};
        CHECK(ssz::encode(a, b));
        CHECK(b == *from_hex(
                       "1500000000000000"
                       "7800000000000000"
                       "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
                       "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
                       "FF000000000000000000EE00000000000000000000EE000000000000000000FF"));
        CHECK(test::decode_success<BeaconBlockHeader>(to_hex(b)) == a);
    }
    SECTION("decoding error") {
        CHECK(test::decode_failure<BeaconBlockHeader>("") == DecodingError::kUnexpectedLength);
        CHECK(test::decode_failure<BeaconBlockHeader>("00") == DecodingError::kUnexpectedLength);
        CHECK(test::decode_failure<BeaconBlockHeader>("1500000000000000"
                                                      "7800000000000000"
                                                      "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
                                                      "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
                                                      "FF000000000000000000EE00000000000000000000EE000000000000000000")
              == DecodingError::kUnexpectedLength);
    }
    /*SECTION("hash_tree_root") {
        BeaconBlockHeader a{
            4,
            3,
            0x56186d7d3ccb9f4b8ac734ca20659fad8e0156506320de524bf18dec9cea82ad_bytes32,
            0xb72cec27c1d30034c03f2961a9b5570b78bc9ca7351ba59503780f2452ccbe09_bytes32,
            0xaf161ca1794d0f6e97c6e766741b1ccd19ea02cb4d477efc546dd6eaf07fdc05_bytes32
        };
        Bytes b{};
        CHECK(ssz::encode(a, b));
        CHECK(b == *from_hex(
                       "0400000000000000"
                       "0300000000000000"
                       "56186d7d3ccb9f4b8ac734ca20659fad8e0156506320de524bf18dec9cea82ad"
                       "b72cec27c1d30034c03f2961a9b5570b78bc9ca7351ba59503780f2452ccbe09"
                       "af161ca1794d0f6e97c6e766741b1ccd19ea02cb4d477efc546dd6eaf07fdc05"));
        CHECK(to_hex(a.hash_tree_root()) == to_hex(0x35d6b35926d73de9ee914b75a01c899d10913c531b983f335e2914388353ca0c_bytes32));
        ssz::HashTree t{*from_hex(
            "0400000000000000000000000000000000000000000000000000000000000000"
                "0300000000000000000000000000000000000000000000000000000000000000"
                "56186d7d3ccb9f4b8ac734ca20659fad8e0156506320de524bf18dec9cea82ad"
                "b72cec27c1d30034c03f2961a9b5570b78bc9ca7351ba59503780f2452ccbe09"
                "af161ca1794d0f6e97c6e766741b1ccd19ea02cb4d477efc546dd6eaf07fdc05"
                "0000000000000000000000000000000000000000000000000000000000000000"
                "0000000000000000000000000000000000000000000000000000000000000000"
                "0000000000000000000000000000000000000000000000000000000000000000"
            )
        };
        CHECK(to_hex(t.root()) == to_hex(0x35d6b35926d73de9ee914b75a01c899d10913c531b983f335e2914388353ca0c_bytes32));
        ssz::HashTree t{*from_hex(
            "0400000000000000000000000000000000000000000000000000000000000000"
            "0300000000000000000000000000000000000000000000000000000000000000"
            )
        };
        CHECK(to_hex(t.root()) == to_hex(0x35d6b35926d73de9ee914b75a01c899d10913c531b983f335e2914388353ca0c_bytes32));

        eth::BeaconBlockHeader aa{
            eth::Slot(4),
            eth::ValidatorIndex(3),
            eth::Root("0x56186d7d3ccb9f4b8ac734ca20659fad8e0156506320de524bf18dec9cea82ad"),
            eth::Root("0xb72cec27c1d30034c03f2961a9b5570b78bc9ca7351ba59503780f2452ccbe09"),
            eth::Root("0xaf161ca1794d0f6e97c6e766741b1ccd19ea02cb4d477efc546dd6eaf07fdc05")
        };
        CHECK(to_hex(aa.hash_tree_root()) == to_hex(0x35d6b35926d73de9ee914b75a01c899d10913c531b983f335e2914388353ca0c_bytes32));
        const auto s = aa.serialize();
        CHECK(Bytes(s.cbegin(), s.cend()) == b);
    }*/
}

TEST_CASE("SignedBeaconBlockHeader SSZ") {
    SECTION("round-trip") {
        SignedBeaconBlockHeader a{
            std::make_unique<BeaconBlockHeader>(BeaconBlockHeader{
                21,
                120,
                0xFF000000000000000000EE00000000000000000000EE000000000000000000FF_bytes32,
                0xFF000000000000000000EE00000000000000000000EE000000000000000000FF_bytes32,
                0xFF000000000000000000EE00000000000000000000EE000000000000000000FF_bytes32
            }),
            {}
        };
        Bytes b{};
        CHECK(ssz::encode(a, b));
        CHECK(b.size() == SignedBeaconBlockHeader::kSize);
        CHECK(b == *from_hex(
                       "1500000000000000"
                       "7800000000000000"
                       "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
                       "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
                       "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"));
        CHECK(test::decode_success<SignedBeaconBlockHeader>(to_hex(b)) == a);
    }
    SECTION("round-trip w/ empty header") {
        SignedBeaconBlockHeader a{
            {},
            {}
        };
        Bytes b{};
        CHECK(ssz::encode(a, b));
        CHECK(b.size() == SignedBeaconBlockHeader::kSize);
        CHECK(b == *from_hex(
                       "0000000000000000"
                       "0000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"));
        CHECK(test::decode_success<SignedBeaconBlockHeader>(to_hex(b)) == a);
    }
    SECTION("decoding error") {
        CHECK(test::decode_failure<SignedBeaconBlockHeader>("") == DecodingError::kUnexpectedLength);
        CHECK(test::decode_failure<SignedBeaconBlockHeader>("00") == DecodingError::kUnexpectedLength);
        CHECK(test::decode_failure<SignedBeaconBlockHeader>("1500000000000000"
                                                            "7800000000000000"
                                                            "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
                                                            "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
                                                            "FF000000000000000000EE00000000000000000000EE00000000000000000000"
                                                            "0000000000000000000000000000000000000000000000000000000000000000"
                                                            "0000000000000000000000000000000000000000000000000000000000000000"
                                                            "00000000000000000000000000000000000000000000000000000000000000")
              == DecodingError::kUnexpectedLength);
    }
}

TEST_CASE("IndexedAttestation SSZ") {
    SECTION("round-trip zero indices") {
        IndexedAttestation a{
            {},
            std::make_unique<AttestationData>(AttestationData{
                120,
                6,
                0xFF000000000000000000EE00000000000000000000EE000000000000000000FF_bytes32,
                std::make_unique<Checkpoint>(Checkpoint{
                    21,
                    0xFF000000000000000000EE00000000000000000000EE000000000000000000FF_bytes32}),
                std::make_unique<Checkpoint>(Checkpoint{
                    21,
                    0xFF000000000000000000EE00000000000000000000EE000000000000000000FF_bytes32}),
            }),
            {}
        };
        Bytes b{};
        CHECK(ssz::encode(a, b));
        CHECK(b == *from_hex(
                       "E4000000"
                       "7800000000000000"
                       "0600000000000000"
                       "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
                       "1500000000000000"
                       "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
                       "1500000000000000"
                       "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"));
        CHECK(test::decode_success<IndexedAttestation>(to_hex(b)) == a);
    }
    SECTION("round-trip two indices") {
        IndexedAttestation a{
            {1, 11},
            std::make_unique<AttestationData>(AttestationData{
                120,
                6,
                0xFF000000000000000000EE00000000000000000000EE000000000000000000FF_bytes32,
                std::make_unique<Checkpoint>(Checkpoint{
                    21,
                    0xFF000000000000000000EE00000000000000000000EE000000000000000000FF_bytes32}),
                std::make_unique<Checkpoint>(Checkpoint{
                    21,
                    0xFF000000000000000000EE00000000000000000000EE000000000000000000FF_bytes32}),
            }),
            {}
        };
        Bytes b{};
        CHECK(ssz::encode(a, b));
        CHECK(b == *from_hex(
                       "E4000000"
                       "7800000000000000"
                       "0600000000000000"
                       "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
                       "1500000000000000"
                       "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
                       "1500000000000000"
                       "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0100000000000000"
                       "0B00000000000000"));
        CHECK(test::decode_success<IndexedAttestation>(to_hex(b)) == a);
    }
    SECTION("round-trip w/ empty source and target") {
        IndexedAttestation a{
            {},
            std::make_unique<AttestationData>(AttestationData{
                120,
                6,
                0xFF000000000000000000EE00000000000000000000EE000000000000000000FF_bytes32,
                {},
                {},
            }),
            {}
        };
        Bytes b{};
        CHECK(ssz::encode(a, b));
        CHECK(b == *from_hex(
                       "E4000000"
                       "7800000000000000"
                       "0600000000000000"
                       "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
                       "0000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"));
        CHECK(test::decode_success<IndexedAttestation>(to_hex(b)) == a);
    }
    SECTION("decoding error") {
        CHECK(test::decode_failure<IndexedAttestation>("") == DecodingError::kInputTooShort);
        CHECK(test::decode_failure<IndexedAttestation>("00") == DecodingError::kInputTooShort);
        CHECK(test::decode_failure<IndexedAttestation>("E4000000"
                                                       "7800000000000000"
                                                       "0600000000000000"
                                                       "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
                                                       "1500000000000000"
                                                       "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
                                                       "1500000000000000"
                                                       "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
                                                       "0000000000000000000000000000000000000000000000000000000000000000"
                                                       "0000000000000000000000000000000000000000000000000000000000000000"
                                                       "00000000000000000000000000000000000000000000000000000000000000")
              == DecodingError::kInputTooShort);
    }
}

TEST_CASE("AttesterSlashing SSZ") {
    Bytes kSerialized{*from_hex(
        "08000000" // 4 - offset0 attester slashing (8)
        "f4000000" // 4 - offset1 attester slashing (8 + 228 + 8)
        "e4000000" // 4 - offset0 indexed attestation (228)
        "0000000000000000" // 8 - slot
        "0000000000000000" // 8 - index
        "0000000000000000000000000000000000000000000000000000000000000000" // 32 - beacon block hash
        "0000000000000000" // 8 - source epoch
        "0000000000000000000000000000000000000000000000000000000000000000" // 32 - source root
        "0000000000000000" // 8 - target epoch
        "0000000000000000000000000000000000000000000000000000000000000000" // 32 - target root
        "0000000000000000000000000000000000000000000000000000000000000000" // 32 - signature 1/3
        "0000000000000000000000000000000000000000000000000000000000000000" // 32 - signature 2/3
        "0000000000000000000000000000000000000000000000000000000000000000" // 32 - signature 3/3
        "0000000000000000" // 8 - attestation indices [0]
        "e4000000" // 4 - offset0 indexed attestation (228)
        "0000000000000000"
        "0000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000" // 8 - attestation indices [0]
        "0000000000000000" // 8 - attestation indices [1]
        "0000000000000000" // 8 - attestation indices [2]
        "0000000000000000" // 8 - attestation indices [3]
        "0000000000000000" // 8 - attestation indices [4]
        "0000000000000000" // 8 - attestation indices [5]
        "0000000000000000" // 8 - attestation indices [6]
        "0000000000000000" // 8 - attestation indices [7]
        )};
    SECTION("encoding round-trip") {
        AttesterSlashing a{
            std::make_unique<IndexedAttestation>(IndexedAttestation{}),
            std::make_unique<IndexedAttestation>(IndexedAttestation{}),
        };
        Bytes b{};
        CHECK(ssz::encode(a, b));
        CHECK(b == *from_hex("08000000" // 4 - offset0 attester slashing (8)
                             "ec000000" // 4 - offset1 attester slashing (8 + 228)
                             "e4000000" // 4 - offset0 indexed attestation (228)
                             "0000000000000000" // 8 - slot
                             "0000000000000000" // 8 - index
                             "0000000000000000000000000000000000000000000000000000000000000000" // 32 - beacon block hash
                             "0000000000000000" // 8 - source epoch
                             "0000000000000000000000000000000000000000000000000000000000000000" // 32 - source root
                             "0000000000000000" // 8 - target epoch
                             "0000000000000000000000000000000000000000000000000000000000000000" // 32 - target root
                             "0000000000000000000000000000000000000000000000000000000000000000" // 32 - signature 1/3
                             "0000000000000000000000000000000000000000000000000000000000000000" // 32 - signature 2/3
                             "0000000000000000000000000000000000000000000000000000000000000000" // 32 - signature 3/3
                             "e4000000" // 4 - offset0 indexed attestation (228)
                             "0000000000000000"
                             "0000000000000000"
                             "0000000000000000000000000000000000000000000000000000000000000000"
                             "0000000000000000"
                             "0000000000000000000000000000000000000000000000000000000000000000"
                             "0000000000000000"
                             "0000000000000000000000000000000000000000000000000000000000000000"
                             "0000000000000000000000000000000000000000000000000000000000000000"
                             "0000000000000000000000000000000000000000000000000000000000000000"
                             "0000000000000000000000000000000000000000000000000000000000000000"
                             ));
        CHECK(test::decode_success<AttesterSlashing>(to_hex(b)) == a);
    }
    SECTION("decoding round-trip") {
        AttesterSlashing a{test::decode_success<AttesterSlashing>(to_hex(kSerialized))};
        AttesterSlashing b{
            std::make_shared<cl::IndexedAttestation>(IndexedAttestation{
                std::vector<uint64_t>{0},
                std::make_shared<cl::AttestationData>(AttestationData{
                    0,
                    0,
                    {},
                    std::make_shared<cl::Checkpoint>(),
                    std::make_shared<cl::Checkpoint>(),
                }),
                {},
            }),
            std::make_shared<cl::IndexedAttestation>(IndexedAttestation{
                std::vector<uint64_t>{0, 0, 0, 0, 0, 0, 0, 0},
                std::make_shared<cl::AttestationData>(AttestationData{
                    0,
                    0,
                    {},
                    std::make_shared<cl::Checkpoint>(),
                    std::make_shared<cl::Checkpoint>(),
                }),
                {},
            }),
        };
        CHECK(a == b);
        CHECK(test::decode_success<AttesterSlashing>(to_hex(kSerialized))== a);
    }
    SECTION("decoding error") {
        CHECK(test::decode_failure<AttesterSlashing>("") == DecodingError::kInputTooShort);
        CHECK(test::decode_failure<AttesterSlashing>("00") == DecodingError::kInputTooShort);
        // Next check truncates the input *one byte before* the attestation indices (8 * sizeof(uit64_t)) producing a corrupt serialization
        CHECK(test::decode_failure<AttesterSlashing>(kSerialized.substr(0, kSerialized.size() - 65)) == DecodingError::kInputTooShort);
    }
}

TEST_CASE("ProposerSlashing SSZ") {
    Bytes kSerialized{*from_hex(
        "1500000000000000"
        "7800000000000000"
        "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
        "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
        "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "2500000000000000"
        "8800000000000000"
        "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
        "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
        "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        )};

    SECTION("encoding round-trip") {
        ProposerSlashing a{
            std::make_unique<SignedBeaconBlockHeader>(),
            std::make_unique<SignedBeaconBlockHeader>(),
        };
        Bytes b{};
        CHECK(ssz::encode(a, b));
        CHECK(b == *from_hex("0000000000000000"
                       "0000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000"
                       "0000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"));
        CHECK(test::decode_success<ProposerSlashing>(to_hex(b)) == a);
    }
    SECTION("decoding round-trip") {
        ProposerSlashing a{test::decode_success<ProposerSlashing>(to_hex(kSerialized))};
        ProposerSlashing b{
            std::make_unique<SignedBeaconBlockHeader>(SignedBeaconBlockHeader{
                std::make_unique<BeaconBlockHeader>(BeaconBlockHeader{
                    21,
                    120,
                    0xFF000000000000000000EE00000000000000000000EE000000000000000000FF_bytes32,
                    0xFF000000000000000000EE00000000000000000000EE000000000000000000FF_bytes32,
                    0xFF000000000000000000EE00000000000000000000EE000000000000000000FF_bytes32
                }),
                {}
            }),
            std::make_unique<SignedBeaconBlockHeader>(SignedBeaconBlockHeader{
                std::make_unique<BeaconBlockHeader>(BeaconBlockHeader{
                    37,
                    136,
                    0xFF000000000000000000EE00000000000000000000EE000000000000000000FF_bytes32,
                    0xFF000000000000000000EE00000000000000000000EE000000000000000000FF_bytes32,
                    0xFF000000000000000000EE00000000000000000000EE000000000000000000FF_bytes32
                }),
                {}
            }),
        };
        CHECK(a == b);
        CHECK(test::decode_success<ProposerSlashing>(to_hex(kSerialized)) == a);
    }
    SECTION("decoding error") {
        CHECK(test::decode_failure<ProposerSlashing>("") == DecodingError::kUnexpectedLength);
        CHECK(test::decode_failure<ProposerSlashing>("00") == DecodingError::kUnexpectedLength);
        // Next check truncates the input *one byte before* the end of the first SignedBeaconBlockHeader producing a corrupt serialization
        CHECK(test::decode_failure<ProposerSlashing>(kSerialized.substr(0, SignedBeaconBlockHeader::kSize - 1)) == DecodingError::kUnexpectedLength);
    }
}

TEST_CASE("Attestation SSZ") {
    SECTION("round-trip one bit") {
        Attestation a{
            {0x01},
            std::make_unique<AttestationData>(AttestationData{
                120,
                6,
                0xFF000000000000000000EE00000000000000000000EE000000000000000000FF_bytes32,
                std::make_unique<Checkpoint>(Checkpoint{
                    21,
                    0xFF000000000000000000EE00000000000000000000EE000000000000000000FF_bytes32}),
                std::make_unique<Checkpoint>(Checkpoint{
                    21,
                    0xFF000000000000000000EE00000000000000000000EE000000000000000000FF_bytes32}),
            }),
            {}
        };
        Bytes b{};
        CHECK(ssz::encode(a, b));
        CHECK(b == *from_hex(
                       "E4000000"
                       "7800000000000000"
                       "0600000000000000"
                       "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
                       "1500000000000000"
                       "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
                       "1500000000000000"
                       "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "01"));
        CHECK(test::decode_success<Attestation>(to_hex(b)) == a);
    }
    SECTION("round-trip two bits") {
        Attestation a{
            {0x01, 0x10},
            std::make_unique<AttestationData>(AttestationData{
                120,
                6,
                0xFF000000000000000000EE00000000000000000000EE000000000000000000FF_bytes32,
                std::make_unique<Checkpoint>(Checkpoint{
                    21,
                    0xFF000000000000000000EE00000000000000000000EE000000000000000000FF_bytes32}),
                std::make_unique<Checkpoint>(Checkpoint{
                    21,
                    0xFF000000000000000000EE00000000000000000000EE000000000000000000FF_bytes32}),
            }),
            {}
        };
        Bytes b{};
        CHECK(ssz::encode(a, b));
        CHECK(b == *from_hex(
                       "E4000000"
                       "7800000000000000"
                       "0600000000000000"
                       "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
                       "1500000000000000"
                       "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
                       "1500000000000000"
                       "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0110"));
        CHECK(test::decode_success<Attestation>(to_hex(b)) == a);
    }
    SECTION("round-trip w/ empty source and target") {
        Attestation a{
            {0x1},
            std::make_unique<AttestationData>(AttestationData{
                120,
                6,
                0xFF000000000000000000EE00000000000000000000EE000000000000000000FF_bytes32,
                {},
                {},
            }),
            {}
        };
        Bytes b{};
        CHECK(ssz::encode(a, b));
        CHECK(b == *from_hex(
                       "E4000000"
                       "7800000000000000"
                       "0600000000000000"
                       "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
                       "0000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "01"));
        CHECK(test::decode_success<Attestation>(to_hex(b)) == a);
    }
    SECTION("decoding error") {
        CHECK(test::decode_failure<Attestation>("") == DecodingError::kInputTooShort);
        CHECK(test::decode_failure<Attestation>("00") == DecodingError::kInputTooShort);
        CHECK(test::decode_failure<Attestation>("E4000000"
                                                "7800000000000000"
                                                "0600000000000000"
                                                "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
                                                "1500000000000000"
                                                "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
                                                "1500000000000000"
                                                "FF000000000000000000EE00000000000000000000EE000000000000000000FF"
                                                "0000000000000000000000000000000000000000000000000000000000000000"
                                                "0000000000000000000000000000000000000000000000000000000000000000"
                                                "0000000000000000000000000000000000000000000000000000000000000000")
              == DecodingError::kInputTooShort);
    }
}

TEST_CASE("DepositData SSZ") {
    SECTION("round-trip zero content") {
        DepositData a{
            {},
            {},
            0,
            {}
        };
        Bytes b{};
        CHECK(ssz::encode(a, b));
        CHECK(b == *from_hex(
                       "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"));
        CHECK(test::decode_success<DepositData>(to_hex(b)) == a);
    }
    SECTION("round-trip w/o signature") {
        DepositData a{
            {0xFF, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0, 0xFF, 0, 0, 0, 0, 0, 0, 0,
             0xFF, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0, 0xFF, 0, 0, 0, 0, 0, 0, 0,
             0xFF, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0, 0xFF, 0},
            {0xAA, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
             0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xBB},
            16,
            {}
        };
        Bytes b{};
        CHECK(ssz::encode(a, b));
        CHECK(b == *from_hex(
                       "FF00000000000000FF00FF00000000000000FF00000000000000FF00FF00000000000000FF00000000000000FF00FF00"
                       "AA000000000000000000000000000000000000000000000000000000000000BB"
                       "1000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"));
        CHECK(test::decode_success<DepositData>(to_hex(b)) == a);
    }
    SECTION("decoding error") {
        CHECK(test::decode_failure<DepositData>("") == DecodingError::kUnexpectedLength);
        CHECK(test::decode_failure<DepositData>("00") == DecodingError::kUnexpectedLength);
        CHECK(test::decode_failure<DepositData>("FF00000000000000FF00FF00000000000000FF00000000000000FF00FF00000000000000FF00000000000000FF00FF00"
                                                "AA000000000000000000000000000000000000000000000000000000000000BB"
                                                "1000000000000000"
                                                "0000000000000000000000000000000000000000000000000000000000000000"
                                                "0000000000000000000000000000000000000000000000000000000000000000"
                                                "00000000000000000000000000000000000000000000000000000000000000")
              == DecodingError::kUnexpectedLength);
    }
}

TEST_CASE("Deposit SSZ") {
    const char* kEmptySerialized{
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
    };
    SECTION("round-trip zero content") {
        Deposit a{
            {},
            std::make_shared<DepositData>(DepositData{
                {},
                {},
                0,
                {}
            }),
        };
        Bytes b{};
        CHECK(ssz::encode(a, b));
        CHECK(b == *from_hex(kEmptySerialized));
        CHECK(test::decode_success<Deposit>(to_hex(b)) == a);
    }
    SECTION("decoding error") {
        CHECK(test::decode_failure<Deposit>("") == DecodingError::kUnexpectedLength);
        CHECK(test::decode_failure<Deposit>("00") == DecodingError::kUnexpectedLength);
        Bytes empty_serialized_bytes = *from_hex(kEmptySerialized);
        empty_serialized_bytes = empty_serialized_bytes.substr(0, empty_serialized_bytes.size() - 1);
        CHECK(test::decode_failure<Deposit>(to_hex(empty_serialized_bytes)) == DecodingError::kUnexpectedLength);
    }
}

TEST_CASE("VoluntaryExit SSZ") {
    SECTION("round-trip") {
        VoluntaryExit a{
            21,
            2,
        };
        Bytes b{};
        CHECK(ssz::encode(a, b));
        CHECK(b == *from_hex("15000000000000000200000000000000"));
        CHECK(test::decode_success<VoluntaryExit>(to_hex(b)) == a);
    }
    SECTION("decoding error") {
        CHECK(test::decode_failure<VoluntaryExit>("") == DecodingError::kUnexpectedLength);
        CHECK(test::decode_failure<VoluntaryExit>("00") == DecodingError::kUnexpectedLength);
        CHECK(test::decode_failure<VoluntaryExit>("150000000000000002000000000000")
              == DecodingError::kUnexpectedLength);
    }
}

TEST_CASE("SignedVoluntaryExit SSZ") {
    SECTION("round-trip") {
        SignedVoluntaryExit a{
            std::make_shared<VoluntaryExit>(VoluntaryExit{
                21,
                2,
            }),
            {},
        };
        Bytes b{};
        CHECK(ssz::encode(a, b));
        CHECK(b == *from_hex(
                       "15000000000000000200000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"
                       "0000000000000000000000000000000000000000000000000000000000000000"));
        CHECK(test::decode_success<SignedVoluntaryExit>(to_hex(b)) == a);
    }
    SECTION("decoding error") {
        CHECK(test::decode_failure<SignedVoluntaryExit>("") == DecodingError::kUnexpectedLength);
        CHECK(test::decode_failure<SignedVoluntaryExit>("00") == DecodingError::kUnexpectedLength);
        CHECK(test::decode_failure<SignedVoluntaryExit>("150000000000000002000000000000"
                                                        "0000000000000000000000000000000000000000000000000000000000000000"
                                                        "0000000000000000000000000000000000000000000000000000000000000000"
                                                        "00000000000000000000000000000000000000000000000000000000000000")
              == DecodingError::kUnexpectedLength);
    }
}

TEST_CASE("SyncAggregate SSZ") {
    const char* kEmptySerialized{
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
    };
    SECTION("round-trip") {
        SyncAggregate a{
            {},
            {},
        };
        CHECK(a.count_commitee_bits() == 0);
        Bytes b{};
        CHECK(ssz::encode(a, b));
        CHECK(b == *from_hex(kEmptySerialized));
        CHECK(test::decode_success<SyncAggregate>(to_hex(b)) == a);
    }
    SECTION("decoding error") {
        CHECK(test::decode_failure<SyncAggregate>("") == DecodingError::kUnexpectedLength);
        CHECK(test::decode_failure<SyncAggregate>("00") == DecodingError::kUnexpectedLength);
        Bytes empty_serialized_bytes = *from_hex(kEmptySerialized);
        empty_serialized_bytes = empty_serialized_bytes.substr(0, empty_serialized_bytes.size() - 1);
        CHECK(test::decode_failure<SyncAggregate>(to_hex(empty_serialized_bytes)) == DecodingError::kUnexpectedLength);
    }
    SECTION("commitee bits") {
        SyncAggregate a{
            {0x7E, 0x12, 0xFF},
            {},
        };
        CHECK(a.count_commitee_bits() == 16);
    }
}

}  // namespace silkworm::cl