// Package s3io implements io interfaces for AWS S3 objects.
package s3io

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var _ io.ReadSeekCloser = (*S3File)(nil)
var _ io.ReaderAt = (*S3File)(nil)

type S3File struct {
	ctx     context.Context
	client  *s3.Client
	options *s3.GetObjectInput
	size    int64
	offset  int64
}

func NewS3File(client *s3.Client, options *s3.GetObjectInput) (*S3File, error) {
	return NewS3FileWithContext(context.Background(), client, options)
}

func NewS3FileWithContext(
	ctx context.Context,
	client *s3.Client,
	options *s3.GetObjectInput,
) (*S3File, error) {
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
		return nil, err
	}
	return &S3File{
		ctx:     ctx,
		client:  client,
		options: options,
		size:    output.ContentLength,
		offset:  0,
	}, nil
}

// Close implements io.Closer
// This is a no-op.
func (*S3File) Close() error {
	return nil
}

// Read implements io.Reader
func (f *S3File) Read(p []byte) (n int, err error) {
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
		return
	}
	defer func() {
		closeErr := output.Body.Close()
		if err == nil {
			err = closeErr
		}
	}()
	n, err = output.Body.Read(p)
	f.offset += int64(n)
	return
}

// Seek implements io.ReadSeeker
func (f *S3File) Seek(offset int64, whence int) (newOffset int64, err error) {
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
func (f *S3File) ReadAt(p []byte, off int64) (n int, err error) {
	cpy := *f
	if _, err = cpy.Seek(off, 0); err != nil {
		return 0, err
	}
	cpy.offset = off
	return io.ReadFull(&cpy, p)
}

func (f *S3File) Copy(dst io.Writer) (int64, error) {
	return f.CopyBuffer(dst, nil)
}

func (f *S3File) CopyBuffer(dst io.Writer, buf []byte) (int64, error) {
	if f.offset >= f.size {
		return 0, io.EOF
	}
	httpRange := fmt.Sprintf("bytes=%d-%d", f.offset, f.size)
	options := *f.options
	options.Range = &httpRange
	output, err := f.client.GetObject(f.ctx, &options)
	if err != nil {
		return 0, err
	}
	defer func() {
		closeErr := output.Body.Close()
		if err == nil {
			err = closeErr
		}
	}()
	return io.CopyBuffer(dst, output.Body, buf)
}

func (f *S3File) CopyN(dst io.Writer, n int64) (int64, error) {
	if f.offset >= f.size {
		return 0, io.EOF
	}
	length := n
	if f.offset+length > f.size {
		length = f.size - f.offset
	}
	httpRange := fmt.Sprintf("bytes=%d-%d", f.offset, f.offset+length)
	options := *f.options
	options.Range = &httpRange
	output, err := f.client.GetObject(f.ctx, &options)
	if err != nil {
		return 0, err
	}
	defer func() {
		closeErr := output.Body.Close()
		if err == nil {
			err = closeErr
		}
	}()
	return io.CopyN(dst, output.Body, n)
}
