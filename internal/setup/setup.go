package setup

// TODO: write this file later
import "github.com/hashicorp/raft"

// import (
// 	"fmt"
// 	r "rflite/internal/raft"
// 	"rflite/internal/sql"
// 	"time"

// 	"github.com/hashicorp/raft"
// )

func SetupLeader() (*raft.Raft, error) {
	// 	fsm := sql.NewSQLFSM("testdata/db_single.db")
	// 	_, trans := raft.NewInmemTransport("master") // master
	// 	// TODO: use proper bind address
	// 	r, err := r.NewTestRaftNode("master", "127.0.0.1:13000", fsm, trans, true) //
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	if err := waitForLeader(r, 2*time.Second); err != nil {
	// 		return nil, err
	// 	}
	// 	return r, nil
	// }

	//	func waitForLeader(r *raft.Raft, timeout time.Duration) error {
	//		deadline := time.Now().Add(timeout)
	//		for time.Now().Before(deadline) {
	//			if r.State() == raft.Leader {
	//				return nil
	//			}
	//			time.Sleep(10 * time.Millisecond)
	//		}
	//		return fmt.Errorf("leader not elected")
	return nil, nil
}
