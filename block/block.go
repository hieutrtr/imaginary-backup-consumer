package block

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/noahdesu/go-ceph/rados"
)

// Block structure
type Block struct {
	contexts map[string]*rados.IOContext
	conn     *rados.Conn
}

var (
	pools         = os.Getenv("CEPH_POOLS")
	cephConfig    = os.Getenv("CEPH_CONF")
	cephBlockPath = os.Getenv("CEPH_BLOCK_PATH")
)

var block *Block

const MAX_OBJECT_BYTE = 20971520

func init() {
	defer func() {
		if r := recover(); r != nil {
			block.Shutdown()
			log.Print(r)
		}
	}()

	block = NewCephBlock()
	block.RegisterContext(pools)
}

// Shutdown ceph connection
func (blk *Block) Shutdown() {
	if blk.conn != nil {
		defer blk.conn.Shutdown()
		for _, ctx := range blk.contexts {
			ctx.Destroy()
		}
	}
}

// NewCephConnection for imaginary
func NewCephBlock() *Block {
	if cephConfig == "" {
		exitWithError("missing CEPH_CONF env")
	}
	if cephBlockPath == "" {
		exitWithError("missing CEPH_BLOCK_PATH env")
	}
	blk := &Block{}
	blk.conn, _ = rados.NewConn()
	blk.conn.ReadConfigFile(cephConfig) // Specify config
	blk.conn.Connect()
	return blk
}

// RegisterContext for imaginary
func (blk *Block) RegisterContext(pools string) {
	if pools == "" {
		exitWithError("missing CEPH_POOLS env")
	}
	poolsArray := strings.Split(pools, ",")
	blk.contexts = make(map[string]*rados.IOContext, len(poolsArray))
	var err error
	for _, pool := range poolsArray {
		log.Print(pool)
		blk.contexts[pool], err = blk.conn.OpenIOContext(pool)
		if err != nil {
			exitWithError("Can not open context " + pool + " with error " + fmt.Sprintln(err))
		}
	}
}

// Transfer ceph object to block or disk
func Transfer(pool, oid string) error {
	buf, err := block.fetchObject(pool, oid)
	if err != nil {
		fmt.Println("Can not fetch object " + pool)
		return err
	}
	path := fmt.Sprintf("/%s/%s", pool, oid)
	err = postToBlock(path, buf)
	if err != nil {
		return err
	}
	fmt.Println("Transfered file to: ", cephBlockPath+path)
	return nil
}

// Restore from block to object storage
func Restore(pool, oid string) error {
	path := fmt.Sprintf("/%s/%s", pool, oid)
	data, err := fetchBlock(path)
	if err != nil {
		fmt.Println("Can not fetch from block path " + path)
		return err
	}
	return block.pushObject(pool, oid, data)
}

func postToBlock(path string, buf []byte) error {
	url := fmt.Sprintf("/%s/%s", cephBlockPath, path)
	return ioutil.WriteFile(url, buf, 0644)
}

func fetchBlock(path string) ([]byte, error) {
	url := fmt.Sprintf("/%s/%s", cephBlockPath, path)
	return ioutil.ReadFile(url)
}

func (blk *Block) pushObject(pool, oid string, data []byte) error {
	return blk.contexts[pool].SetXattr(oid, "data", data)
}

func (blk *Block) fetchObject(pool, oid string) ([]byte, error) {
	data := make([]byte, MAX_OBJECT_BYTE)
	leng, _ := blk.contexts[pool].GetXattr(oid, "data", data)
	return data[:leng], nil
}

func exitWithError(mess string) {
	panic("block: " + mess)
}
