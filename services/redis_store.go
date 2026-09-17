package services

import (
	"bytes"
	"context"
	"encoding/gob"
	"io"
	"net/http"
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

// isNotFound reports whether err means "the object is not there". S3
// spells that three ways: NoSuchKey on a GET, NotFound on a HEAD-shaped
// reply, and — from implementations that send neither code — a bare HTTP
// 404. Reading a 404 as a hard failure would make the runner treat a
// missing artifact as an unreachable store and give up.
func isNotFound(err error) bool {
	var rf awserr.RequestFailure
	if errors.As(err, &rf) && rf.StatusCode() == http.StatusNotFound {
		return true
	}
	var ae awserr.Error
	if errors.As(err, &ae) {
		switch ae.Code() {
		case s3.ErrCodeNoSuchKey, "NotFound":
			return true
		}
	}
	return false
}

func (r *RedisStore) GetFinal(ctx context.Context, key string) ([]byte, bool, error) {
	if !r.useS3 {
		b, err := r.rc.Get().Get(ctx, "tr:final:"+key).Bytes()
		if errors.Is(err, redis.Nil) {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, errors.Wrap(err, "redis get final")
		}
		return b, true, nil
	}
	out, err := r.s3c.Get().GetObjectWithContext(ctx, &s3.GetObjectInput{Bucket: aws.String(r.bucket), Key: aws.String(r.prefix + key + ".vtt")})
	if err != nil {
		if isNotFound(err) {
			return nil, false, nil
		}
		return nil, false, errors.Wrap(err, "s3 get")
	}
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, false, errors.Wrap(err, "s3 read")
	}
	return b, true, nil
}

func (r *RedisStore) PutFinal(ctx context.Context, key string, vtt []byte) error {
	if !r.useS3 {
		return errors.Wrap(r.rc.Get().Set(ctx, "tr:final:"+key, vtt, 0).Err(), "redis put final")
	}
	_, err := r.s3c.Get().PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String(r.bucket), Key: aws.String(r.prefix + key + ".vtt"),
		Body: bytes.NewReader(vtt), ContentType: aws.String("text/vtt; charset=utf-8"),
	})
	return errors.Wrap(err, "s3 put")
}

// posTTL is how long a viewer's position outlives their last poll. Long
// enough to survive a paused poll (the player suspends it on pause and on a
// hidden tab), short enough that a job resumed a day later starts in file
// order instead of at a position nobody is at any more.
const posTTL = 30 * time.Minute

func (r *RedisStore) PutPos(ctx context.Context, key string, pos time.Duration) error {
	return errors.Wrap(r.rc.Get().Set(ctx, "tr:pos:"+key, pos.Milliseconds(), posTTL).Err(), "redis put pos")
}

func (r *RedisStore) GetPos(ctx context.Context, key string) (time.Duration, bool, error) {
	ms, err := r.rc.Get().Get(ctx, "tr:pos:"+key).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, errors.Wrap(err, "redis get pos")
	}
	return time.Duration(ms) * time.Millisecond, true, nil
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
	return errors.Wrap(r.rc.Get().Set(ctx, "tr:cues:"+key, buf.Bytes(), progressTTL).Err(), "redis put progress")
}

func (r *RedisStore) DropProgress(ctx context.Context, key string) error {
	return errors.Wrap(r.rc.Get().Del(ctx, "tr:cues:"+key).Err(), "redis drop progress")
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
