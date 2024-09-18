package accesslog_aliyun_oss

import (
	"net/http"

	"github.com/wzshiming/httpcache"
)

func NewCacheHTTPClient(cli *http.Client, cachePath string) *http.Client {
	if cli == nil {
		cli = http.DefaultClient
	}
	return httpcache.NewClient(cli,
		httpcache.WithStorer(httpcache.DirectoryStorer(cachePath)),
		httpcache.WithKeyer(httpcache.JointKeyer(
			httpcache.KeyerFunc(func(req *http.Request) string {
				return req.URL.Path
			}),
		)),
		httpcache.WithFilterer(httpcache.FiltererFunc(func(req *http.Request) bool {
			if req.URL.Path == "/" {
				return false
			}

			if req.Method != http.MethodGet {
				return false
			}
			return true
		})),
	)
}
