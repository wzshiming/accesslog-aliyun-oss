package main

import (
	"log"
	"os"
	"time"

	"github.com/spf13/pflag"
	"github.com/wzshiming/accesslog"
	accesslog_aliyun_oss "github.com/wzshiming/accesslog-aliyun-oss"
	"github.com/wzshiming/accesslog/tocsv"
)

var (
	cache           = "./cache"
	endpoint        string
	bucket          string
	accessKeyID     string
	accessKeySecret string

	startTime string
	endTime   string

	fields    = accesslog_aliyun_oss.AccessLogFormatted{}.Fields()
	condition string
)

var DataFormat = "2006-01-02-15"

func init() {
	now := time.Now()
	endTime = now.Add(-time.Hour).Format(DataFormat)
	startTime = now.AddDate(0, 0, -1).Format(DataFormat)

	pflag.StringVar(&cache, "cache", cache, "cache")
	pflag.StringVar(&endpoint, "endpoint", "", "endpoint")
	pflag.StringVar(&bucket, "bucket", "", "bucket")
	pflag.StringVar(&accessKeyID, "access-key-id", "", "access key id")
	pflag.StringVar(&accessKeySecret, "access-key-secret", "", "access key secret")
	pflag.StringVar(&startTime, "start-time", startTime, "start time")
	pflag.StringVar(&endTime, "end-time", endTime, "end time")
	pflag.StringSliceVar(&fields, "field", fields, "fields")
	pflag.StringVar(&condition, "condition", condition, "condition")
	pflag.Parse()
}

func main() {
	err := run(
		fields,
		condition,
		startTime,
		endTime,
		endpoint,
		bucket,
		accessKeyID,
		accessKeySecret,
		cache,
	)
	if err != nil {
		log.Fatal(err)
	}
}

const dataFormat = "2006-01-02-15"

func run(
	fields []string,
	condition string,
	startTime string,
	endTime string,
	endpoint string,
	bucketName string,
	accessKeyID string,
	accessKeySecret string,
	cache string,
) error {
	end, err := time.Parse(dataFormat, endTime)
	if err != nil {
		return err
	}

	start, err := time.Parse(dataFormat, startTime)
	if err != nil {
		return err
	}

	cacheHTTPClient := accesslog_aliyun_oss.NewCacheHTTPClient(nil, cache)

	client, err := accesslog_aliyun_oss.NewOSSClient(
		endpoint,
		accessKeyID,
		accessKeySecret,
		accesslog_aliyun_oss.HTTPClient(cacheHTTPClient),
	)
	if err != nil {
		return err
	}

	ch := make(chan accesslog_aliyun_oss.AccessLogFormatted, 128)

	go func() {
		defer close(ch)
		for i := start; i.Before(end); i = i.Add(time.Hour) {
			date := i.Format(dataFormat) + "-"
			err := accesslog_aliyun_oss.ProcessAccessLogWithClient(client, bucketName, date, func(entry accesslog.Entry[accesslog_aliyun_oss.AccessLog], err error) error {
				if err != nil {
					return err
				}
				f, err := entry.Entry().Formatted()
				if err != nil {
					return err
				}
				ch <- f
				return nil
			})
			if err != nil {
				log.Fatal("Error", err)
			}
		}
	}()

	return tocsv.ProcessToCSV[accesslog_aliyun_oss.AccessLogFormatted](os.Stdout, condition, fields, ch)
}
