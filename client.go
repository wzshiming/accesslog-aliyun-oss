package accesslog_aliyun_oss

import (
	"fmt"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/wzshiming/accesslog"
)

type Client = oss.Client

var (
	HTTPClient   = oss.HTTPClient
	NewOSSClient = oss.New
)

func ProcessAccessLogWithClient(client *Client, bucketName, dateStr string, callback func(entry accesslog.Entry[AccessLog], err error) error) error {
	bucketLoggingResult, err := client.GetBucketLogging(bucketName)
	if err != nil {
		return fmt.Errorf("failed to get bucket: %w", err)
	}

	bucket, err := client.Bucket(bucketLoggingResult.LoggingEnabled.TargetBucket)
	if err != nil {
		return fmt.Errorf("failed to get bucket: %w", err)
	}

	prefix := bucketLoggingResult.LoggingEnabled.TargetPrefix + bucketName + dateStr
	continueToken := ""
	for {
		lsRes, err := bucket.ListObjectsV2(
			oss.ContinuationToken(continueToken),
			oss.Prefix(prefix),
		)

		if err != nil {
			return fmt.Errorf("failed to list objects: %w", err)
		}

		for _, object := range lsRes.Objects {
			err := (func() error {
				r, err := bucket.GetObject(object.Key)
				if err != nil {
					return fmt.Errorf("failed to get object: %w", err)
				}
				defer r.Close()

				err = accesslog.ProcessEntries[AccessLog](r, callback)
				if err != nil {
					return err
				}
				return nil
			})()
			if err != nil {
				return err
			}
		}
		if !lsRes.IsTruncated {
			break
		}
		continueToken = lsRes.NextContinuationToken
	}
	return nil
}
