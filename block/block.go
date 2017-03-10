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
var cephConfig = os.Getenv("CEPH_CONF")

func init() {
	RegisterContext()
}

// RegisterContext for imaginary
func RegisterContext() {
	conn, _ = rados.NewConn()
	if baseUrl == "" {
		baseUrl = "images"
	}
	if cephConfig == "" {
		cephConfig = "/etc/ceph/ceph.conf"
	}
	conn.ReadConfigFile(cephConfig) // Specify config
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
	url := fmt.Sprintf("/%s/%s", baseUrl, path)
	return ioutil.WriteFile(url, buf, 0644)
}

// Restore from block to object storage
func Restore(pool, oid string) error {
	path := fmt.Sprintf("/%s/%s", pool, oid)
	data, err := fetchBlock(path)
	if err != nil {
		fmt.Println("Can not fetch from block path " + path)
		return err
	}
	return pushObject(pool, oid, data)
}

func fetchBlock(path string) ([]byte, error) {
	url := fmt.Sprintf("/%s/%s", baseUrl, path)
	return ioutil.ReadFile(url)
}

func pushObject(pool, oid string, data []byte) error {
	return contexts[pool].SetXattr(oid, "data", data)
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
