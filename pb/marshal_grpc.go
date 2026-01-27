//go:build grpc

/*
 * SPDX-FileCopyrightText: 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package pb

import "google.golang.org/protobuf/proto"

// Marshal marshals a proto.Message to bytes.
func Marshal(m proto.Message) ([]byte, error) {
	return proto.Marshal(m)
}

// Unmarshal unmarshals bytes into a proto.Message.
func Unmarshal(data []byte, m proto.Message) error {
	return proto.Unmarshal(data, m)
}

// Size returns the encoded size of a proto.Message.
func Size(m proto.Message) int {
	return proto.Size(m)
}

// MarshalOptions wraps proto.MarshalOptions for compatibility.
type MarshalOptions = proto.MarshalOptions
