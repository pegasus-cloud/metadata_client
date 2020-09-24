package utility

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"
	"unsafe"
)

// SendRequest ...
func SendRequest(method string, url string, headers map[string]string, body interface{}) (respBody []byte, statusCode int, err error) {
	var (
		request *http.Request
		resp    *http.Response
	)
	request, err = bundleRequest(method, url, headers, body)
	if err != nil {
		return nil, -1, err
	}
	resp, err = bundleClient(request, false, nil)

	if err != nil {
		return nil, -1, err
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, err
	}
	return b, resp.StatusCode, err
}

func bundleRequest(method string, url string, headers map[string]string, body interface{}) (request *http.Request, err error) {
	var b io.Reader
	if body != nil {
		switch body.(type) {
		case *bytes.Buffer:
			b = bytes.NewBuffer([]byte(fmt.Sprintf("%v", body)))
		default:
			if b, err = convertBodyType(&body); err != nil {
				return nil, err
			}
		}
	}
	if request, err = http.NewRequest(method, url, b); err != nil {
		return nil, err
	}
	if headers != nil {
		for key, val := range headers {
			request.Header.Add(key, val)
		}
	}
	return request, nil
}

func convertBodyType(body *interface{}) (*strings.Reader, error) {
	if body == nil {
		return nil, nil
	}
	newBodyType, err := json.Marshal(body)

	if err != nil {
		return nil, err
	}
	return strings.NewReader(string(newBodyType)), nil
}

func bundleClient(req *http.Request, withSSL bool, certFile *string) (resp *http.Response, err error) {
	var client *http.Client
	if req.URL.Scheme == "https" {
		var trans *http.Transport
		if certFile != nil {
			trans = withX509File(certFile)
		} else {
			trans = withInsecureVierify()
		}
		client = &http.Client{
			Transport: trans,
			Timeout:   5 * time.Second,
		}
	} else {
		client = &http.Client{
			Timeout: 5 * time.Second,
		}
	}
	resp, err = client.Do(req)
	return
}

func withX509File(certFile *string) *http.Transport {
	f, err := ioutil.ReadFile(*certFile)
	if err != nil {
		log.Fatal(err)
	}
	cert := x509.NewCertPool()
	if ok := cert.AppendCertsFromPEM(f); ok {
		return &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: cert},
		}
	}
	return nil
}

func withInsecureVierify() *http.Transport {
	return &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
}

type stringHeader struct {
	Data unsafe.Pointer
	Len  int
}

type sliceHeader struct {
	Data unsafe.Pointer
	Len  int
	Cap  int
}

func bytesToStr(b []byte) string {
	header := (*sliceHeader)(unsafe.Pointer(&b))
	strHeader := &stringHeader{
		Data: header.Data,
		Len:  header.Len,
	}
	return *(*string)(unsafe.Pointer(strHeader))
}
