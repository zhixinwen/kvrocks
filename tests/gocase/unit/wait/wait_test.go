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
	"strings"
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
		result := masterRdb.Wait(ctx, -1, 0)
		require.Error(t, result.Err())
		require.Contains(t, result.Err().Error(), "numreplicas should be a positive integer")
	})

	t.Run("WAIT with invalid arguments should return error", func(t *testing.T) {
		result := masterRdb.Do(ctx, "WAIT")
		require.Error(t, result.Err())
		require.Contains(t, result.Err().Error(), "wrong number of arguments")

		result = masterRdb.Do(ctx, "WAIT", "1")
		require.Error(t, result.Err())
		require.Contains(t, result.Err().Error(), "wrong number of arguments")
	})

	t.Run("WAIT should not block indefinitely", func(t *testing.T) {
		// Start a goroutine to execute WAIT
		done := make(chan bool, 1)
		go func() {
			require.NoError(t, masterRdb.Set(ctx, "k1", "v1", 0).Err())
			require.NoError(t, masterRdb.Wait(ctx, 1, 0).Err())
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
		require.NoError(t, slaveRdb.Do(ctx, "SLAVEOF", "NO", "ONE").Err())

		// Master remove the slave from the replication list periodically
		// so we need to wait for the master to detect the disconnection
		require.Eventually(t, func() bool {
			info := masterRdb.Info(ctx, "replication").Val()
			return !strings.Contains(info, "connected_slaves:1")
		}, 50*time.Second, 100*time.Millisecond)

		// Start a goroutine to execute WAIT
		done := make(chan bool, 1)
		go func() {
			require.NoError(t, masterRdb.Set(ctx, "k1", "v1", 0).Err())
			require.NoError(t, masterRdb.Wait(ctx, 1, 0).Err())
			done <- true
		}()

		select {
		case <-done:
			t.Fatal("WAIT command did not block")
		case <-time.After(1 * time.Second):
			// Success - command blocked
		}

		// Reconnect the slave
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
}

// When WAIT is executed, it should block future commands in the buffer until the number of replicas that have reached the target sequence is reached.
func TestWaitBlockExecutingCommand(t *testing.T) {
	// Start master server
	masterSrv := util.StartServer(t, map[string]string{})
	defer masterSrv.Close()

	tcpClient := masterSrv.NewTCPClient()
	defer func() { require.NoError(t, tcpClient.Close()) }()

	// should be blocked after k1 is set
	require.NoError(t, tcpClient.WriteArgs("SET", "k1", "v1"))
	require.NoError(t, tcpClient.WriteArgs("WAIT", "1"))

	// sleep for some time, so the commands are not read by a single read callback.
	time.Sleep(1 * time.Second)

	// should be blocked after k1 is set to v1
	require.NoError(t, tcpClient.WriteArgs("SET", "k1", "v2"))
	require.NoError(t, tcpClient.WriteArgs("WAIT", "1"))

	time.Sleep(1 * time.Second)

	require.NoError(t, tcpClient.WriteArgs("SET", "k1", "v3"))

	masterRdb := masterSrv.NewClient()
	defer func() { require.NoError(t, masterRdb.Close()) }()

	// only the first command should be executed
	require.Equal(t, "v1", masterRdb.Get(context.Background(), "k1").Val())

	// Start slave server
	slaveSrv := util.StartServer(t, map[string]string{})
	defer slaveSrv.Close()

	slaveRdb := slaveSrv.NewClient()
	defer func() { require.NoError(t, slaveRdb.Close()) }()

	// Set up replication
	util.SlaveOf(t, slaveRdb, masterSrv)
	util.WaitForOffsetSync(t, masterRdb, slaveRdb, 5*time.Second)

	// the remaing command should be executed after replication
	require.Equal(t, "v3", masterRdb.Get(context.Background(), "k1").Val())
}

func TestContinueExecutingCommandAfterWait(t *testing.T) {

}
