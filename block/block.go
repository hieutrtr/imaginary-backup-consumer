package block

import (
	"bytes"
	"fmt"
	"io"

	"github.com/noahdesu/go-ceph/rados"
)

var contexts map[string]*rados.IOContext
var conn *rados.Conn
var pools = []string{"ads", "avatar_profile", "property_project"}

func init() {
	RegisterContext()
}

// RegisterContext for imaginary
func RegisterContext() {
	conn, _ = rados.NewConn()
	conn.ReadConfigFile("/etc/ceph/ceph.conf")             // Specify config
	conn.SetConfigOption("log_file", "/etc/ceph/ceph.log") // Specify log path
	conn.Connect()
	for _, pool := range pools {
		contexts[pool], _ = connector.OpenIOContext(pool)
	}
}

// Transfer ceph object to block or disk
func Transfer(pool, oid string) error {
	buf, err := fetchObject(pool, oid)
	if err != nil {
		return err
	}
	err = postToBlock(buf)
	if err != nil {
		return err
	}
	return nil
}

func postToBlock(buf []byte) error {
	fmt.Println(string(buf))
	return nil
}

func fetchObject(pool, oid string) ([]byte, error) {
	data := make([]byte, 5242880)
	leng, _ := contexts[pool].GetXattr(oid, "data", data)

	buf := bytes.NewBuffer(make([]byte, 0, leng+1))
	io.Copy(buf, bytes.NewReader(data[:leng]))
	// BonusStep: Get attribute
	return buf.Bytes(), nil
}

func Close() {
	for pool := range pools {
		contexts[pool].Destroy()
	}
	conn.Shutdown()
}
