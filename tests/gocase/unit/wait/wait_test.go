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
	// Start master server
	masterSrv := util.StartServer(t, map[string]string{})
	defer masterSrv.Close()

	// Start slave server
	slaveSrv := util.StartServer(t, map[string]string{})
	defer slaveSrv.Close()

	ctx := context.Background()
	masterRdb := masterSrv.NewClient()
	defer func() { require.NoError(t, masterRdb.Close()) }()

	slaveRdb := slaveSrv.NewClient()
	defer func() { require.NoError(t, slaveRdb.Close()) }()

	// Set up replication
	util.SlaveOf(t, slaveRdb, masterSrv)
	util.WaitForSync(t, slaveRdb)

	t.Run("WAIT with negative number should return error", func(t *testing.T) {
		result := masterRdb.Do(ctx, "WAIT", "-1")
		require.Error(t, result.Err())
		require.Contains(t, result.Err().Error(), "numreplicas should be a positive integer")
	})

	t.Run("WAIT with invalid arguments should return error", func(t *testing.T) {
		result := masterRdb.Do(ctx, "WAIT")
		require.Error(t, result.Err())
		require.Contains(t, result.Err().Error(), "wrong number of arguments")

		result = masterRdb.Do(ctx, "WAIT", "1", "1000")
		require.Error(t, result.Err())
		require.Contains(t, result.Err().Error(), "wrong number of arguments")
	})

	t.Run("WAIT should not block indefinitely", func(t *testing.T) {
		// Start a goroutine to execute WAIT
		done := make(chan bool, 1)
		go func() {
			require.NoError(t, masterRdb.Do(ctx, "MULTI").Err())
			require.NoError(t, masterRdb.Do(ctx, "SET", "k1", "v1").Err())
			require.NoError(t, masterRdb.Do(ctx, "WAIT", "1").Err())
			require.Equal(t, []interface{}([]interface{}{"OK", int64(1)}), masterRdb.Do(ctx, "EXEC").Val())
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

	t.Run("WAIT should block until enough replicas acknowledge", func(t *testing.T) {
		// Disconnect the slave
		slaveSrv.Close()

		// Start a goroutine to execute WAIT
		done := make(chan bool, 1)
		go func() {
			require.NoError(t, masterRdb.Do(ctx, "MULTI").Err())
			require.NoError(t, masterRdb.Do(ctx, "SET", "k1", "v1").Err())
			require.NoError(t, masterRdb.Do(ctx, "WAIT", "1").Err())
			require.Equal(t, []interface{}([]interface{}{"OK", int64(1)}), masterRdb.Do(ctx, "EXEC").Val())
			done <- true
		}()

		select {
		case <-done:
			t.Fatal("WAIT command did not block")
		default:
			// Success - command is blocked
		}

		// Restart slave and reconnect
		slaveSrv.Start()
		slaveRdb = slaveSrv.NewClient()
		util.SlaveOf(t, slaveRdb, masterSrv)
		util.WaitForSync(t, slaveRdb)

		// Now WAIT should complete
		select {
		case <-done:
			// Success - command completed after replica connected
		case <-time.After(5 * time.Second):
			t.Fatal("WAIT command did not complete after replica connected")
		}
	})

	t.Run("WAIT in script should not block indefinitely", func(t *testing.T) {
		// Create a Lua script that uses WAIT
		script := `
			redis.call('SET', 'k1', 'v1')
			return redis.call('WAIT', 1)
		`

		// Start a goroutine to execute the script
		done := make(chan bool, 1)
		go func() {
			result := masterRdb.Eval(ctx, script, []string{})
			require.NoError(t, result.Err())
			done <- true
		}()

		// Wait for the script to complete (should be immediate)
		select {
		case <-done:
			// Success - script completed immediately
		case <-time.After(5 * time.Second):
			t.Fatal("WAIT in script blocked indefinitely")
		}
	})

	t.Run("WAIT in script should block until enough replicas acknowledge", func(t *testing.T) {
		// Disconnect the slave
		slaveSrv.Close()

		// Create a Lua script that uses WAIT
		script := `
			redis.call('SET', 'k2', 'v2')
			return redis.call('WAIT', 1)
		`

		// Start a goroutine to execute the script
		done := make(chan bool, 1)
		go func() {
			result := masterRdb.Eval(ctx, script, []string{})
			require.NoError(t, result.Err())
			done <- true
		}()

		select {
		case <-done:
			t.Fatal("WAIT in script did not block")
		default:
			// Success - script is blocked
		}

		// Restart slave and reconnect
		slaveSrv.Start()
		slaveRdb = slaveSrv.NewClient()
		util.SlaveOf(t, slaveRdb, masterSrv)
		util.WaitForSync(t, slaveRdb)

		// Now WAIT in script should complete
		select {
		case <-done:
			// Success - script completed after replica connected
		case <-time.After(5 * time.Second):
			t.Fatal("WAIT in script did not complete after replica connected")
		}
	})
}
