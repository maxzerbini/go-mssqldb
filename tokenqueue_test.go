// +build bq

package mssql

import (
	"testing"
	"time"
)

// go test -v -tags bq | grep BlockingQueue

func Test_newTokenStructBlockingQueue(t *testing.T) {
	bq := newTokenStructBlockingQueue()
	err := bq.close()
	if err != nil {
		t.Fatalf("BlockingQueue queue not initialized properly")
	}
}

func Test_tokenStructBlockingQueue_put(t *testing.T) {
	bq := newTokenStructBlockingQueue()
	token := "HELLO"
	done := bq.put(token)
	if !done {
		t.Fatalf("BlockingQueue put failed")
	}
	err := bq.close()
	if err != nil {
		t.Fatalf("BlockingQueue queue not initialized properly")
	}
}

func Test_tokenStructBlockingQueue_pop(t *testing.T) {
	bq := newTokenStructBlockingQueue()
	token := "HELLO"
	done := bq.put(token)
	if !done {
		t.Fatalf("BlockingQueue put failed")
	}
	tk, done := bq.pop()
	if !done {
		t.Fatalf("BlockingQueue pup failed")
	}
	if tk != nil {
		t.Logf("BlockingQueue Token: %v", tk)
	} else {
		t.Fatalf("BlockingQueue pop failed")
	}
	err := bq.close()
	if err != nil {
		t.Fatalf("BlockingQueue queue not initialized properly")
	}
}

func Test_tokenStructBlockingQueue_parallel_put_pop(t *testing.T) {
	bq := newTokenStructBlockingQueue()
	go func() {
		t.Logf("BlockingQueue: PUT sleeping ...")
		time.Sleep(2 * time.Second)
		done := bq.put("token #1")
		if !done {
			t.Fatalf("BlockingQueue put failed")
		}
		t.Logf("BlockingQueue: PUT token 1")
		done = bq.put("token #2")
		if !done {
			t.Fatalf("BlockingQueue put failed")
		}
		t.Logf("BlockingQueue: PUT token 2")
		done = bq.put("token #3")
		if !done {
			t.Fatalf("BlockingQueue put failed")
		}
		t.Logf("BlockingQueue: PUT token 3")
		done = bq.put("token #4")
		if !done {
			t.Fatalf("BlockingQueue put failed")
		}
		t.Logf("BlockingQueue: PUT token 4")
		t.Logf("BlockingQueue: PUT sleeping ...")
		time.Sleep(2 * time.Second)
		bq.close()
		t.Logf("BlockingQueue: PUT close queue")
	}()
	var tk tokenStruct
	var done bool = true
	for done {
		t.Logf("BlockingQueue: calling pop ...")
		tk, done = bq.pop()
		if done {
			if tk != nil {
				t.Logf("BlockingQueue: POP token = %v", tk)
			} else {
				t.Fatalf("BlockingQueue pop failed")
			}
		} else {
			t.Logf("BlockingQueue: closed")
		}
	}
	t.Logf("BlockingQueue: test done")

}
