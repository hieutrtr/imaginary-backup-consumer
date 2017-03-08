package block

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/noahdesu/go-ceph/rados"
)

var contexts map[string]*rados.IOContext
var conn *rados.Conn
var pools = []string{"ads", "profile_avatar", "property_project"}
var baseUrl = os.Getenv("BLOCK_URL")

func init() {
	RegisterContext()
}

// RegisterContext for imaginary
func RegisterContext() {
	conn, _ = rados.NewConn()
	conn.ReadConfigFile("/etc/ceph/ceph.conf")             // Specify config
	conn.SetConfigOption("log_file", "/etc/ceph/ceph.log") // Specify log path
	conn.Connect()
	contexts = make(map[string]*rados.IOContext, len(pools))
	var err error
	for _, pool := range pools {
		contexts[pool], err = conn.OpenIOContext(pool)
		if err != nil {
			fmt.Println("Can not open context " + pool)
		}
	}
}

// Transfer ceph object to block or disk
func Transfer(pool, oid string) error {
	buf, err := fetchObject(pool, oid)
	if err != nil {
		fmt.Println("Can not fetch object " + pool)
		return err
	}
	path := fmt.Sprintf("/%s/%s", pool, oid)
	err = postToBlock(path, buf)
	if err != nil {
		return err
	}
	fmt.Println("Transfered file to: ", baseUrl+path)
	return nil
}

func postToBlock(path string, buf []byte) error {
	return ioutil.WriteFile(baseUrl+path, buf, 0644)
}

func fetchObject(pool, oid string) ([]byte, error) {
	data := make([]byte, 5242880)
	leng, _ := contexts[pool].GetXattr(oid, "data", data)

	buf := bytes.NewBuffer(make([]byte, 0, leng+1))
	io.Copy(buf, bytes.NewReader(data[:leng]))
	// BonusStep: Get attribute
	return buf.Bytes(), nil
}

// Close all contexts and connection
func Close() {
	for _, pool := range pools {
		contexts[pool].Destroy()
	}
	conn.Shutdown()
}
