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

func TestWait(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("WAIT command basic functionality", func(t *testing.T) {
		// Test with no replicas - should return immediately
		r := rdb.Do(ctx, "WAIT", "1", "1000")
		require.NoError(t, r.Err())
		require.Equal(t, int64(0), r.Val())

		// Test with timeout 0 - should return immediately
		r = rdb.Do(ctx, "WAIT", "1", "0")
		require.NoError(t, r.Err())
		require.Equal(t, int64(0), r.Val())
	})

	t.Run("WAIT command with invalid arguments", func(t *testing.T) {
		// Test with wrong number of arguments
		r := rdb.Do(ctx, "WAIT", "1")
		require.Error(t, r.Err())
		require.Contains(t, r.Err().Error(), "wrong number of arguments")

		r = rdb.Do(ctx, "WAIT", "1", "1000", "extra")
		require.Error(t, r.Err())
		require.Contains(t, r.Err().Error(), "wrong number of arguments")

		// Test with invalid numreplicas
		r = rdb.Do(ctx, "WAIT", "invalid", "1000")
		require.Error(t, r.Err())
		require.Contains(t, r.Err().Error(), "should be a positive integer")

		r = rdb.Do(ctx, "WAIT", "-1", "1000")
		require.Error(t, r.Err())
		require.Contains(t, r.Err().Error(), "should be a positive integer")

		// Test with invalid timeout
		r = rdb.Do(ctx, "WAIT", "1", "invalid")
		require.Error(t, r.Err())
		require.Contains(t, r.Err().Error(), "should be a positive integer")

		r = rdb.Do(ctx, "WAIT", "1", "-1")
		require.Error(t, r.Err())
		require.Contains(t, r.Err().Error(), "should be a positive integer")
	})

	t.Run("WAIT command timeout behavior", func(t *testing.T) {
		// Test timeout behavior - should return after timeout with 0 replicas
		start := time.Now()
		r := rdb.Do(ctx, "WAIT", "1", "100") // 100ms timeout
		require.NoError(t, r.Err())
		require.Equal(t, int64(0), r.Val())
		duration := time.Since(start)

		// Should take at least 100ms
		require.GreaterOrEqual(t, duration, 100*time.Millisecond)
		// But should not take too long
		require.Less(t, duration, 500*time.Millisecond)
	})
}
