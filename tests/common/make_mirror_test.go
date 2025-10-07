// Copyright 2016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/tests/v3/framework/config"
	"go.etcd.io/etcd/tests/v3/framework/testutils"
)

func TestMakeMirrorModifyDestPrefix(t *testing.T) {
	testRunner.BeforeTest(t)
	for _, srcTC := range clusterTestCases() {
		for _, destTC := range clusterTestCases() {
			t.Run(fmt.Sprintf("Source=%s/Destination=%s", srcTC.name, destTC.name), func(t *testing.T) {

				var (
					mmOpts     = config.MakeMirrorOptions{Prefix: "o_", DestPrefix: "d_"}
					sourcekvs  = []testutils.KV{{Key: "o_key1", Val: "val1"}, {Key: "o_key2", Val: "val2"}, {Key: "o_key3", Val: "val3"}}
					destkvs    = []testutils.KV{{Key: "d_key1", Val: "val1"}, {Key: "d_key2", Val: "val2"}, {Key: "d_key3", Val: "val3"}}
					srcprefix  = "o_"
					destprefix = "d_"
				)

				testMirror(t, srcTC, destTC, mmOpts, sourcekvs, destkvs, srcprefix, destprefix)
			})
		}
	}
}

func TestMakeMirrorNoDestPrefix(t *testing.T) {
	testRunner.BeforeTest(t)
	for _, srcTC := range clusterTestCases() {
		for _, destTC := range clusterTestCases() {

			t.Run(fmt.Sprintf("Source=%s/Destination=%s", srcTC.name, destTC.name), func(t *testing.T) {
				var (
					mmOpts     = config.MakeMirrorOptions{Prefix: "o_", NoDestPrefix: true}
					sourcekvs  = []testutils.KV{{Key: "o_key1", Val: "val1"}, {Key: "o_key2", Val: "val2"}, {Key: "o_key3", Val: "val3"}}
					destkvs    = []testutils.KV{{Key: "key1", Val: "val1"}, {Key: "key2", Val: "val2"}, {Key: "key3", Val: "val3"}}
					srcprefix  = "o_"
					destprefix = "key"
				)

				testMirror(t, srcTC, destTC, mmOpts, sourcekvs, destkvs, srcprefix, destprefix)
			})
		}
	}
}

func TestMakeMirror(t *testing.T) {
	testRunner.BeforeTest(t)
	for _, srcTC := range clusterTestCases() {
		for _, destTC := range clusterTestCases() {
			t.Run(fmt.Sprintf("Source=%s/Destination=%s", srcTC.name, destTC.name), func(t *testing.T) {
				var (
					mmOpts    config.MakeMirrorOptions
					sourcekvs = []testutils.KV{{Key: "key1", Val: "val1"}, {Key: "key2", Val: "val2"}, {Key: "key3", Val: "val3"}}
					destkvs   = []testutils.KV{{Key: "key1", Val: "val1"}, {Key: "key2", Val: "val2"}, {Key: "key3", Val: "val3"}}
					prefix    = "key"
				)

				testMirror(t, srcTC, destTC, mmOpts, sourcekvs, destkvs, prefix, prefix)

			})
		}
	}
}

func TestMakeMirrorWithWatchRev(t *testing.T) {
	testRunner.BeforeTest(t)
	for _, srcTC := range clusterTestCases() {
		for _, destTC := range clusterTestCases() {
			t.Run(fmt.Sprintf("Source=%s/Destination=%s", srcTC.name, destTC.name), func(t *testing.T) {
				var (
					mmOpts     = config.MakeMirrorOptions{Prefix: "o_", NoDestPrefix: true, Rev: 4}
					sourcekvs  = []testutils.KV{{Key: "o_key1", Val: "val1"}, {Key: "o_key2", Val: "val2"}, {Key: "o_key3", Val: "val3"}, {Key: "o_key4", Val: "val4"}}
					destkvs    = []testutils.KV{{Key: "key3", Val: "val3"}, {Key: "key4", Val: "val4"}}
					srcprefix  = "o_"
					destprefix = "key"
				)

				testMirror(t, srcTC, destTC, mmOpts, sourcekvs, destkvs, srcprefix, destprefix)

			})
		}
	}
}

func testMirror(t *testing.T, srcTC, destTC testCase, mmOpts config.MakeMirrorOptions, sourcekvs, destkvs []testutils.KV, srcprefix, destprefix string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	// Source cluster
	src := testRunner.NewCluster(ctx, t, config.WithClusterConfig(srcTC.config))
	defer src.Close()
	srcClient := testutils.MustClient(src.Client())

	// Destination cluster
	//different value to avoid collisions
	// destcfg := destTC.config
	// e2ectx := e2e.ensureE2EClusterContext(&destcfg)
	// e2ectx.BasePort = 10001
	// destcfg.BasePort := 10001
	dest := testRunner.NewCluster(ctx, t, config.WithClusterConfig(destTC.config))
	defer dest.Close()
	destClient := testutils.MustClient(dest.Client())

	// Start make mirror
	go func() {
		_ <- srcClient.MakeMirror(ctx, dest.Endpoints(), mmOpts)
	}()

	// Write to source
	for i := range sourcekvs {
		require.NoError(t, srcClient.Put(ctx, sourcekvs[i].Key, sourcekvs[i].Val, config.PutOptions{}))
	}

	// Source assertion
	_, err := srcClient.Get(ctx, srcprefix, config.GetOptions{Prefix: true})
	require.NoError(t, err)

	// Destination assertion
	watchChan := destClient.Watch(ctx, destprefix, config.WatchOptions{Prefix: true, Revision: 1})

}
