package services

import (
	"bytes"
	"context"
	"encoding/gob"
	"io"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	"github.com/urfave/cli"
	cs "github.com/webtor-io/common-services"
)

const (
	flagUseS3    = "use-s3"
	flagBucket   = "aws-bucket"
	flagS3Prefix = "s3-prefix"
	progressTTL  = 24 * time.Hour
)

func RegisterStoreFlags(f []cli.Flag) []cli.Flag {
	return append(f,
		cli.BoolFlag{Name: flagUseS3, Usage: "store finished translations in S3", EnvVar: "USE_S3"},
		cli.StringFlag{Name: flagBucket, Usage: "S3 bucket (one bucket per service)", Value: "subtitle-translate", EnvVar: "AWS_BUCKET"},
		cli.StringFlag{Name: flagS3Prefix, Usage: "optional S3 key prefix", Value: "", EnvVar: "S3_PREFIX"},
	)
}

type RedisStore struct {
	rc     *cs.RedisClient
	s3c    *cs.S3Client
	useS3  bool
	bucket string
	prefix string
}

func NewRedisStore(c *cli.Context, rc *cs.RedisClient, s3c *cs.S3Client) *RedisStore {
	return &RedisStore{rc: rc, s3c: s3c, useS3: c.Bool(flagUseS3) && s3c != nil, bucket: c.String(flagBucket), prefix: c.String(flagS3Prefix)}
}

func (r *RedisStore) GetFinal(ctx context.Context, key string) ([]byte, bool, error) {
	if !r.useS3 {
		b, err := r.rc.Get().Get(ctx, "tr:final:"+key).Bytes()
		if errors.Is(err, redis.Nil) {
			return nil, false, nil
		}
		return b, err == nil, err
	}
	out, err := r.s3c.Get().GetObjectWithContext(ctx, &s3.GetObjectInput{Bucket: aws.String(r.bucket), Key: aws.String(r.prefix + key + ".vtt")})
	if err != nil {
		if ae, ok := err.(awserr.Error); ok && ae.Code() == s3.ErrCodeNoSuchKey {
			return nil, false, nil
		}
		return nil, false, errors.Wrap(err, "s3 get")
	}
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	return b, err == nil, err
}

func (r *RedisStore) PutFinal(ctx context.Context, key string, vtt []byte) error {
	if !r.useS3 {
		return r.rc.Get().Set(ctx, "tr:final:"+key, vtt, 0).Err()
	}
	_, err := r.s3c.Get().PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String(r.bucket), Key: aws.String(r.prefix + key + ".vtt"),
		Body: bytes.NewReader(vtt), ContentType: aws.String("text/vtt; charset=utf-8"),
	})
	return errors.Wrap(err, "s3 put")
}

func (r *RedisStore) GetProgress(ctx context.Context, key string) (*Progress, error) {
	b, err := r.rc.Get().Get(ctx, "tr:cues:"+key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.Wrap(err, "redis get progress")
	}
	var p Progress
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&p); err != nil {
		return nil, errors.Wrap(err, "decode progress")
	}
	return &p, nil
}

func (r *RedisStore) PutProgress(ctx context.Context, key string, p *Progress) error {
	buf := &bytes.Buffer{}
	if err := gob.NewEncoder(buf).Encode(p); err != nil {
		return errors.Wrap(err, "encode progress")
	}
	return r.rc.Get().Set(ctx, "tr:cues:"+key, buf.Bytes(), progressTTL).Err()
}

func (r *RedisStore) DropProgress(ctx context.Context, key string) error {
	return r.rc.Get().Del(ctx, "tr:cues:"+key).Err()
}

// refreshLockScript and unlockScript are compare-and-act: they touch the
// key only while it still carries this holder's token, so a job that lost
// its lock cannot extend or delete its successor's.
const refreshLockScript = `if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pexpire", KEYS[1], ARGV[2]) else return 0 end`

const unlockScript = `if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end`

func (r *RedisStore) TryLock(ctx context.Context, key string, ttl time.Duration) (string, bool, error) {
	token, err := newLockToken()
	if err != nil {
		return "", false, err
	}
	ok, err := r.rc.Get().SetNX(ctx, "tr:lock:"+key, token, ttl).Result()
	if err != nil {
		return "", false, errors.Wrap(err, "redis lock")
	}
	if !ok {
		return "", false, nil
	}
	return token, true, nil
}

func (r *RedisStore) RefreshLock(ctx context.Context, key string, token string, ttl time.Duration) (bool, error) {
	n, err := r.rc.Get().Eval(ctx, refreshLockScript, []string{"tr:lock:" + key}, token, ttl.Milliseconds()).Int64()
	if err != nil {
		return false, errors.Wrap(err, "redis refresh lock")
	}
	return n == 1, nil
}

func (r *RedisStore) Unlock(ctx context.Context, key string, token string) error {
	if err := r.rc.Get().Eval(ctx, unlockScript, []string{"tr:lock:" + key}, token).Err(); err != nil {
		return errors.Wrap(err, "redis unlock")
	}
	return nil
}
