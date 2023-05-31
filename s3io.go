// Package s3io implements io interfaces for AWS S3 objects.
package s3io

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const uploadPartSize = 8 * 1024 * 1024

var (
	ErrNotExist = fs.ErrNotExist // "file does not exist"
)

var (
	_ io.ReadSeekCloser = (*S3FileReader)(nil)
	_ io.ReaderAt       = (*S3FileReader)(nil)
	_ io.WriteCloser    = (*S3FileWriter)(nil)
)

type S3FileReader struct {
	ctx     context.Context
	client  *s3.Client
	options *s3.GetObjectInput
	size    int64
	offset  int64
}

func OpenFile(
	client *s3.Client,
	options *s3.GetObjectInput,
) (*S3FileReader, error) {
	return OpenFileWithContext(context.Background(), client, options)
}

func OpenFileWithContext(
	ctx context.Context,
	client *s3.Client,
	options *s3.GetObjectInput,
) (*S3FileReader, error) {
	output, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:               options.Bucket,
		Key:                  options.Key,
		ChecksumMode:         options.ChecksumMode,
		ExpectedBucketOwner:  options.ExpectedBucketOwner,
		IfMatch:              options.IfMatch,
		IfModifiedSince:      options.IfModifiedSince,
		IfNoneMatch:          options.IfNoneMatch,
		IfUnmodifiedSince:    options.IfUnmodifiedSince,
		PartNumber:           options.PartNumber,
		RequestPayer:         options.RequestPayer,
		SSECustomerAlgorithm: options.SSECustomerAlgorithm,
		SSECustomerKey:       options.SSECustomerKey,
		SSECustomerKeyMD5:    options.SSECustomerKeyMD5,
		VersionId:            options.VersionId,
	})
	if err != nil {
		if isNotExist(err) {
			err = ErrNotExist
		}
		return nil, err
	}
	return &S3FileReader{
		ctx:     ctx,
		client:  client,
		options: options,
		size:    output.ContentLength,
		offset:  0,
	}, nil
}

// Close implements io.Closer
// This is a no-op.
func (*S3FileReader) Close() error {
	return nil
}

// Read implements io.Reader
func (f *S3FileReader) Read(p []byte) (n int, err error) {
	if f.offset >= f.size {
		err = io.EOF
		return
	}
	length := int64(len(p))
	if f.offset+length > f.size {
		length = f.size - f.offset
	}
	httpRange := fmt.Sprintf("bytes=%d-%d", f.offset, f.offset+length)
	options := *f.options
	options.Range = &httpRange
	var output *s3.GetObjectOutput
	output, err = f.client.GetObject(f.ctx, &options)
	if err != nil {
		if isNotExist(err) {
			err = ErrNotExist
		}
		return
	}
	defer func() {
		closeErr := output.Body.Close()
		if err == nil {
			err = closeErr
		}
	}()
	if n, err = output.Body.Read(p); err != nil {
		if isNotExist(err) {
			err = ErrNotExist
		}
		return
	}
	f.offset += int64(n)
	return
}

// Seek implements io.ReadSeeker
func (f *S3FileReader) Seek(
	offset int64,
	whence int,
) (newOffset int64, err error) {
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = f.offset + offset
	case io.SeekEnd:
		newOffset = f.size + offset
	default:
		return 0, errors.New("invalid value for argument `whence`")
	}
	if newOffset < 0 {
		return 0, errors.New("seek position is before start of file")
	}
	f.offset = newOffset
	return f.offset, nil
}

// ReadAt implements io.ReaderAt
func (f *S3FileReader) ReadAt(p []byte, off int64) (n int, err error) {
	cpy := *f
	if _, err = cpy.Seek(off, 0); err != nil {
		return 0, err
	}
	cpy.offset = off
	return io.ReadFull(&cpy, p)
}

type S3FileWriter struct {
	pipeR *io.PipeReader
	pipeW *io.PipeWriter
	err   <-chan error
	ctx   context.Context
}

func CreateFileWith(
	client *s3.Client,
	options *s3.PutObjectInput,
) (*S3FileWriter, error) {
	return CreateFileWithContext(
		context.Background(),
		client,
		options,
	)
}

func CreateFileWithContext(
	ctx context.Context,
	client *s3.Client,
	options *s3.PutObjectInput,
) (f *S3FileWriter, err error) {
	f = new(S3FileWriter)
	f.pipeR, f.pipeW = io.Pipe()
	optsCpy := *options
	optsCpy.Body = f.pipeR
	uploader := manager.NewUploader(client, func(u *manager.Uploader) {
		u.PartSize = uploadPartSize
	})
	errc := make(chan error)
	f.err = errc
	f.ctx = ctx
	go func() {
		_, err := uploader.Upload(ctx, &optsCpy)
		closeErr := f.pipeR.CloseWithError(err)
		if err == nil {
			err = closeErr
		}
		select {
		case errc <- err:
		case <-ctx.Done():
		}
	}()
	return
}

// Write implements io.WriteCloser
func (f *S3FileWriter) Write(p []byte) (n int, err error) {
	return f.pipeW.Write(p)
}

// Close implements io.WriteCloser
func (f *S3FileWriter) Close() error {
	var err error
	select {
	case err = <-f.err:
	case <-f.ctx.Done():
		err = context.Cause(f.ctx)
	}
	closeErr := f.pipeW.Close()
	if err == nil {
		err = closeErr
	}
	return err
}

func (f *S3FileWriter) CloseWithError(err error) error {
	return f.pipeW.CloseWithError(err)
}

func isNotExist(err error) bool {
	var responseError *awshttp.ResponseError
	return errors.As(err, &responseError) &&
		responseError.ResponseError.HTTPStatusCode() == http.StatusNotFound
}
