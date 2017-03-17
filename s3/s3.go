package s3

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"

	s3 "github.com/minio/minio-go"
)

var (
	s3EndPoint        = os.Getenv("S3_ENDPOINT")
	s3AccessKeyID     = os.Getenv("S3_ACCESS_KEY_ID")
	s3SecretAccessKey = os.Getenv("S3_SECRET_ACCESS_KEY")
	s3UseSSL          = os.Getenv("S3_USE_SSL")
	s3ObjContentType  = "application/octet-stream"
	baseURL           = os.Getenv("BLOCK_URL")
)

var client *s3.Client

func init() {
	var err error
	var useSSL bool
	if useSSL, err = strconv.ParseBool(s3UseSSL); err != nil {
		useSSL = false
	}
	client, err = s3.New(s3EndPoint, s3AccessKeyID, s3SecretAccessKey, useSSL)
	if err != nil {
		log.Fatalln("Fail to connect S3 %s", fmt.Sprint(err))
		os.Exit(1)
	}
}

func fetchBlock(path string) ([]byte, error) {
	url := fmt.Sprintf("/%s/%s", baseURL, path)
	return ioutil.ReadFile(url)
}

func Restore(service, oid string) error {
	path := fmt.Sprintf("/%s/%s", service, oid)
	data, err := fetchBlock(path)
	if err != nil {
		fmt.Println("Can not fetch from block path " + path)
		return err
	}
	service = strings.Replace(service, "_", "-", -1)
	fmt.Println(service, oid)
	_, err = client.PutObject(service, oid, bytes.NewReader(data), s3ObjContentType)
	return err
}
