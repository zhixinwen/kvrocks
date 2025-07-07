/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package wait

import (
	"context"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestWaitCommand(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("WAIT with no replicas should return immediately", func(t *testing.T) {
		// WAIT 1 should return immediately since there are no replicas
		result := rdb.Do(ctx, "WAIT", "1")
		require.NoError(t, result.Err())
		require.Equal(t, int64(0), result.Val())
	})

	t.Run("WAIT with negative number should return error", func(t *testing.T) {
		result := rdb.Do(ctx, "WAIT", "-1")
		require.Error(t, result.Err())
		require.Contains(t, result.Err().Error(), "numreplicas should be a positive integer")
	})

	t.Run("WAIT with invalid arguments should return error", func(t *testing.T) {
		result := rdb.Do(ctx, "WAIT")
		require.Error(t, result.Err())
		require.Contains(t, result.Err().Error(), "wrong number of arguments")

		result = rdb.Do(ctx, "WAIT", "1", "1000")
		require.Error(t, result.Err())
		require.Contains(t, result.Err().Error(), "wrong number of arguments")
	})

	t.Run("WAIT should work with valid arguments", func(t *testing.T) {
		// This should return immediately since there are no replicas
		result := rdb.Do(ctx, "WAIT", "2")
		require.NoError(t, result.Err())
		require.Equal(t, int64(0), result.Val())
	})

	t.Run("WAIT should not block indefinitely", func(t *testing.T) {
		// Start a goroutine to execute WAIT
		done := make(chan bool, 1)
		go func() {
			result := rdb.Do(ctx, "WAIT", "1")
			require.NoError(t, result.Err())
			require.Equal(t, int64(0), result.Val())
			done <- true
		}()

		// Wait for the command to complete (should be immediate)
		select {
		case <-done:
			// Success - command completed immediately
		case <-time.After(5 * time.Second):
			t.Fatal("WAIT command blocked indefinitely")
		}
	})
}
