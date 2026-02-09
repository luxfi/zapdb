//go:build !grpc

/*
 * SPDX-FileCopyrightText: 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package pb

import (
	"testing"
)

func TestKVMarshalUnmarshal(t *testing.T) {
	kv := &KV{
		Key:        []byte("test-key"),
		Value:      []byte("test-value"),
		UserMeta:   []byte{0x01},
		Version:    12345,
		ExpiresAt:  67890,
		Meta:       []byte{0x02},
		StreamId:   42,
		StreamDone: true,
	}

	data, err := kv.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	kv2 := &KV{}
	if err := kv2.Unmarshal(data); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if string(kv2.Key) != string(kv.Key) {
		t.Errorf("Key mismatch: got %s, want %s", kv2.Key, kv.Key)
	}
	if string(kv2.Value) != string(kv.Value) {
		t.Errorf("Value mismatch: got %s, want %s", kv2.Value, kv.Value)
	}
	if kv2.Version != kv.Version {
		t.Errorf("Version mismatch: got %d, want %d", kv2.Version, kv.Version)
	}
	if kv2.ExpiresAt != kv.ExpiresAt {
		t.Errorf("ExpiresAt mismatch: got %d, want %d", kv2.ExpiresAt, kv.ExpiresAt)
	}
	if kv2.StreamId != kv.StreamId {
		t.Errorf("StreamId mismatch: got %d, want %d", kv2.StreamId, kv.StreamId)
	}
	if kv2.StreamDone != kv.StreamDone {
		t.Errorf("StreamDone mismatch: got %v, want %v", kv2.StreamDone, kv.StreamDone)
	}
}

func TestKVListMarshalUnmarshal(t *testing.T) {
	list := &KVList{
		Kv: []*KV{
			{Key: []byte("key1"), Value: []byte("value1"), Version: 1},
			{Key: []byte("key2"), Value: []byte("value2"), Version: 2},
		},
		AllocRef: 999,
	}

	data, err := list.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	list2 := &KVList{}
	if err := list2.Unmarshal(data); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if len(list2.Kv) != len(list.Kv) {
		t.Fatalf("KV count mismatch: got %d, want %d", len(list2.Kv), len(list.Kv))
	}
	if list2.AllocRef != list.AllocRef {
		t.Errorf("AllocRef mismatch: got %d, want %d", list2.AllocRef, list.AllocRef)
	}
	for i := range list.Kv {
		if string(list2.Kv[i].Key) != string(list.Kv[i].Key) {
			t.Errorf("KV[%d].Key mismatch", i)
		}
	}
}

func TestManifestChangeMarshalUnmarshal(t *testing.T) {
	mc := &ManifestChange{
		Id:             12345,
		Op:             ManifestChange_CREATE,
		Level:          3,
		KeyId:          67890,
		EncryptionAlgo: EncryptionAlgo_aes,
		Compression:    1,
	}

	data, err := mc.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	mc2 := &ManifestChange{}
	if err := mc2.Unmarshal(data); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if mc2.Id != mc.Id {
		t.Errorf("Id mismatch: got %d, want %d", mc2.Id, mc.Id)
	}
	if mc2.Op != mc.Op {
		t.Errorf("Op mismatch: got %d, want %d", mc2.Op, mc.Op)
	}
	if mc2.Level != mc.Level {
		t.Errorf("Level mismatch: got %d, want %d", mc2.Level, mc.Level)
	}
}

func TestChecksumMarshalUnmarshal(t *testing.T) {
	cs := &Checksum{
		Algo: Checksum_XXHash64,
		Sum:  123456789012345,
	}

	data, err := cs.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	cs2 := &Checksum{}
	if err := cs2.Unmarshal(data); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if cs2.Algo != cs.Algo {
		t.Errorf("Algo mismatch: got %d, want %d", cs2.Algo, cs.Algo)
	}
	if cs2.Sum != cs.Sum {
		t.Errorf("Sum mismatch: got %d, want %d", cs2.Sum, cs.Sum)
	}
}

func TestDataKeyMarshalUnmarshal(t *testing.T) {
	dk := &DataKey{
		KeyId:     123,
		Data:      []byte("encryption-key-data"),
		Iv:        []byte("initialization-vector"),
		CreatedAt: 1609459200,
	}

	data, err := dk.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	dk2 := &DataKey{}
	if err := dk2.Unmarshal(data); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if dk2.KeyId != dk.KeyId {
		t.Errorf("KeyId mismatch: got %d, want %d", dk2.KeyId, dk.KeyId)
	}
	if string(dk2.Data) != string(dk.Data) {
		t.Errorf("Data mismatch")
	}
	if dk2.CreatedAt != dk.CreatedAt {
		t.Errorf("CreatedAt mismatch: got %d, want %d", dk2.CreatedAt, dk.CreatedAt)
	}
}

func TestMarshalUnmarshalInterface(t *testing.T) {
	kv := &KV{
		Key:     []byte("test"),
		Value:   []byte("value"),
		Version: 100,
	}

	// Test via interface
	data, err := Marshal(kv)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	kv2 := &KV{}
	if err := Unmarshal(data, kv2); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if kv2.Version != kv.Version {
		t.Errorf("Version mismatch")
	}

	// Test Size
	if Size(kv) != len(data) {
		t.Errorf("Size mismatch: got %d, want %d", Size(kv), len(data))
	}
}

func TestMarshalAppend(t *testing.T) {
	kv := &KV{
		Key:     []byte("test"),
		Value:   []byte("value"),
		Version: 100,
	}

	prefix := []byte("prefix-")
	result, err := MarshalOptions{}.MarshalAppend(prefix, kv)
	if err != nil {
		t.Fatalf("MarshalAppend failed: %v", err)
	}

	if string(result[:7]) != "prefix-" {
		t.Errorf("Prefix not preserved")
	}

	kv2 := &KV{}
	if err := kv2.Unmarshal(result[7:]); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if kv2.Version != kv.Version {
		t.Errorf("Version mismatch")
	}
}

func TestKVClone(t *testing.T) {
	kv := &KV{
		Key:        []byte("test-key"),
		Value:      []byte("test-value"),
		UserMeta:   []byte{0x01},
		Version:    12345,
		ExpiresAt:  67890,
		Meta:       []byte{0x02},
		StreamId:   42,
		StreamDone: true,
	}

	clone := kv.Clone()

	// Verify values are equal
	if string(clone.Key) != string(kv.Key) {
		t.Errorf("Key mismatch")
	}
	if clone.Version != kv.Version {
		t.Errorf("Version mismatch")
	}

	// Verify deep copy (modifying original doesn't affect clone)
	kv.Key[0] = 'x'
	if clone.Key[0] == 'x' {
		t.Errorf("Clone shares memory with original")
	}
}
