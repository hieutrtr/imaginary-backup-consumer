package block

import (
	"bytes"
	"fmt"
	"io"

	"github.com/noahdesu/go-ceph/rados"
)

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
	connector, err := rados.NewConn()
	connector.ReadConfigFile("/etc/ceph/ceph.conf")             // Specify config
	connector.SetConfigOption("log_file", "/etc/ceph/ceph.log") // Specify log path
	connector.Connect()                                         // Start connection
	defer connector.Shutdown()
	ioctx, err := connector.OpenIOContext(pool) // Step2: Open IO context
	if err != nil {
		return nil, err
	}
	defer ioctx.Destroy()

	data := make([]byte, 5242880)
	leng, err := ioctx.GetXattr(oid, "data", data)

	buf := bytes.NewBuffer(make([]byte, 0, leng+1))
	io.Copy(buf, bytes.NewReader(data[:leng]))
	// BonusStep: Get attribute
	return buf.Bytes(), nil
}
