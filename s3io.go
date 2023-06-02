package s3io

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"strings"
	"time"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var (
	_ io.Reader   = (*s3Reader)(nil)
	_ io.ReaderAt = (*s3Reader)(nil)
	_ io.Seeker   = (*s3Reader)(nil)
	_ io.Closer   = (*s3Reader)(nil)
	_ io.Writer   = (*s3Writer)(nil)
	_ io.Closer   = (*s3Writer)(nil)
)

func Open(
	bucket, key string,
	opts ...ReadOption,
) (*s3Reader, error) {
	o := ReadOptions{
		GetObjectInput: s3.GetObjectInput{
			Bucket: &bucket,
			Key:    &key,
		},
	}
	return OpenWithOptions(o, opts...)
}

func OpenWithURI(
	URI string,
	opts ...ReadOption,
) (*s3Reader, error) {
	if !strings.HasPrefix(strings.ToLower(URI), "s3://") {
		URI = "s3://" + strings.TrimPrefix(URI, "/")
	}
	u, err := url.Parse(URI)
	if err != nil {
		return nil, err
	}
	return Open(u.Host, u.Path, opts...)
}

func OpenWithOptions(
	o ReadOptions,
	opts ...ReadOption,
) (*s3Reader, error) {
	for _, opt := range opts {
		opt.ApplyReadOption(&o)
	}
	if o.GetObjectInput.Bucket == nil {
		return nil, errors.New("must specify bucket")
	}
	if o.GetObjectInput.Key == nil {
		return nil, errors.New("must specify key in bucket")
	}

	if o.ctx == nil {
		o.ctx = context.Background()
	}

	if o.client == nil && o.downloader == nil {
		var err error
		o.client, err = getDefaultClient(o.ctx)
		if err != nil {
			return nil, err
		}
	}

	type headFetcher interface {
		HeadObject(
			ctx context.Context,
			params *s3.HeadObjectInput,
			optFns ...func(*s3.Options),
		) (*s3.HeadObjectOutput, error)
	}

	var headClient headFetcher
	if o.client != nil {
		headClient = o.client
	} else if o.downloader != nil {
		var ok bool
		if headClient, ok = o.downloader.S3.(headFetcher); !ok {
			return nil, errors.New(
				"downloader field `S3` is missing function `HeadObject(...)`",
			)
		}
	}

	output, err := headClient.HeadObject(o.ctx, &s3.HeadObjectInput{
		Bucket:               o.GetObjectInput.Bucket,
		Key:                  o.GetObjectInput.Key,
		ChecksumMode:         o.GetObjectInput.ChecksumMode,
		ExpectedBucketOwner:  o.GetObjectInput.ExpectedBucketOwner,
		IfMatch:              o.GetObjectInput.IfMatch,
		IfModifiedSince:      o.GetObjectInput.IfModifiedSince,
		IfNoneMatch:          o.GetObjectInput.IfNoneMatch,
		IfUnmodifiedSince:    o.GetObjectInput.IfUnmodifiedSince,
		PartNumber:           o.GetObjectInput.PartNumber,
		RequestPayer:         o.GetObjectInput.RequestPayer,
		SSECustomerAlgorithm: o.GetObjectInput.SSECustomerAlgorithm,
		SSECustomerKey:       o.GetObjectInput.SSECustomerKey,
		SSECustomerKeyMD5:    o.GetObjectInput.SSECustomerKeyMD5,
		VersionId:            o.GetObjectInput.VersionId,
	})

	if err != nil {
		if isNotExist(err) {
			err = fs.ErrNotExist
		}
		return nil, err
	}
	return &s3Reader{
		ctx:        o.ctx,
		client:     o.client,
		downloader: o.downloader,
		input:      o.GetObjectInput,
		size:       output.ContentLength,
		buf:        o.buf,
		bufOffset:  -1,
	}, nil
}

type ReadOptions struct {
	s3.GetObjectInput
	ctx        context.Context
	client     *s3.Client
	downloader *manager.Downloader
	buf        []byte
}

type ReadOption interface {
	ApplyReadOption(*ReadOptions)
}

func Create(
	bucket, key string,
	opts ...WriteOption,
) (*s3Writer, error) {
	o := WriteOptions{
		PutObjectInput: s3.PutObjectInput{
			Bucket: &bucket,
			Key:    &key,
		},
	}
	return CreateWithOptions(o, opts...)
}

func CreateWithURI(URI string, opts ...WriteOption) (*s3Writer, error) {
	if !strings.HasPrefix(strings.ToLower(URI), "s3://") {
		URI = "s3://" + strings.TrimPrefix(URI, "/")
	}
	u, err := url.Parse(URI)
	if err != nil {
		return nil, err
	}
	return Create(u.Host, u.Path, opts...)
}

func CreateWithOptions(
	o WriteOptions,
	opts ...WriteOption,
) (*s3Writer, error) {
	for _, opt := range opts {
		opt.ApplyWriteOption(&o)
	}
	if o.PutObjectInput.Bucket == nil {
		return nil, errors.New("must specify bucket")
	}
	if o.PutObjectInput.Key == nil {
		return nil, errors.New("must specify key in bucket")
	}
	if o.PutObjectInput.Body != nil {
		return nil, errors.New("Options.PutObjectInput.Body must be nil")
	}
	if o.ctx == nil {
		o.ctx = context.Background()
	}

	if o.client == nil && o.uploader == nil {
		var err error
		o.client, err = getDefaultClient(o.ctx)
		if err != nil {
			return nil, err
		}
	}
	if o.uploader == nil {
		// getting error 501 when using client to upload
		// directly, use uploader manager always
		o.uploader = manager.NewUploader(o.client, func(u *manager.Uploader) {
			u.Concurrency = 1
		})
	}
	pipeR, pipeW := io.Pipe()
	o.PutObjectInput.Body = pipeR
	errc := make(chan error)
	go func() {
		var err error
		_, err = o.uploader.Upload(o.ctx, &o.PutObjectInput)
		closeErr := pipeR.CloseWithError(err)
		if err == nil {
			err = closeErr
		}
		select {
		case errc <- err:
		case <-o.ctx.Done():
		}
	}()
	return &s3Writer{
		ctx:   o.ctx,
		pipeW: pipeW,
		err:   errc,
	}, nil
}

type WriteOptions struct {
	s3.PutObjectInput
	ctx      context.Context
	client   *s3.Client
	uploader *manager.Uploader
}

type WriteOption interface {
	ApplyWriteOption(*WriteOptions)
}

func Remove(
	bucket, key string,
	opts ...DeleteOption,
) error {
	o := DeleteOptions{
		DeleteObjectInput: s3.DeleteObjectInput{
			Bucket: &bucket,
			Key:    &key,
		},
	}
	return RemoveWithOptions(o, opts...)
}

func RemoveWithURI(URI string, opts ...DeleteOption) error {
	if !strings.HasPrefix(strings.ToLower(URI), "s3://") {
		URI = "s3://" + strings.TrimPrefix(URI, "/")
	}
	u, err := url.Parse(URI)
	if err != nil {
		return err
	}
	return Remove(u.Host, u.Path, opts...)
}

func RemoveWithOptions(
	o DeleteOptions,
	opts ...DeleteOption,
) error {
	for _, opt := range opts {
		opt.ApplyDeleteOption(&o)
	}
	if o.DeleteObjectInput.Bucket == nil {
		return errors.New("must specify bucket")
	}
	if o.DeleteObjectInput.Key == nil {
		return errors.New("must specify key in bucket")
	}
	if o.ctx == nil {
		o.ctx = context.Background()
	}
	var err error
	if o.client == nil {
		if o.client, err = getDefaultClient(o.ctx); err != nil {
			return err
		}
	}
	outp, err := o.client.DeleteObject(o.ctx, &o.DeleteObjectInput)
	if isNotExist(err) {
		err = fs.ErrNotExist
	}
	_ = outp
	return err
}

type DeleteOptions struct {
	s3.DeleteObjectInput
	ctx    context.Context
	client *s3.Client
}

type DeleteOption interface {
	ApplyDeleteOption(*DeleteOptions)
}

func Stat(
	bucket, key string,
	opts ...StatOption,
) (*fileInfo, error) {
	o := StatOptions{
		HeadObjectInput: s3.HeadObjectInput{
			Bucket: &bucket,
			Key:    &key,
		},
	}
	return StatWithOptions(o, opts...)
}

func StatWithURI(
	URI string,
	opts ...StatOption,
) (*fileInfo, error) {
	if !strings.HasPrefix(strings.ToLower(URI), "s3://") {
		URI = "s3://" + strings.TrimPrefix(URI, "/")
	}
	u, err := url.Parse(URI)
	if err != nil {
		return nil, err
	}
	return Stat(u.Host, u.Path, opts...)
}

func StatWithOptions(
	o StatOptions,
	opts ...StatOption,
) (*fileInfo, error) {
	for _, opt := range opts {
		opt.ApplyStatOption(&o)
	}
	if o.HeadObjectInput.Bucket == nil {
		return nil, errors.New("must specify bucket")
	}
	if o.HeadObjectInput.Key == nil {
		return nil, errors.New("must specify key in bucket")
	}
	if o.ctx == nil {
		o.ctx = context.Background()
	}
	var err error
	if o.client == nil {
		if o.client, err = getDefaultClient(o.ctx); err != nil {
			return nil, err
		}
	}
	var output *s3.HeadObjectOutput
	output, err = o.client.HeadObject(o.ctx, &o.HeadObjectInput)
	if err != nil {
		if isNotExist(err) {
			err = fs.ErrNotExist
		}
		return nil, err
	}
	return &fileInfo{
		HeadObjectOutput: *output,
		key:              *o.Key,
	}, nil
}

var _ fs.FileInfo = new(fileInfo)

type fileInfo struct {
	s3.HeadObjectOutput
	key string
}

// IsDir implements fs.FileInfo.
func (*fileInfo) IsDir() bool {
	return false
}

// ModTime implements fs.FileInfo.
func (i *fileInfo) ModTime() time.Time {
	return *i.LastModified
}

// Mode implements fs.FileInfo.
func (*fileInfo) Mode() fs.FileMode {
	return fs.FileMode(0)
}

// Name implements fs.FileInfo.
func (i *fileInfo) Name() string {
	return i.key
}

// Size implements fs.FileInfo.
func (i *fileInfo) Size() int64 {
	return i.ContentLength
}

// Sys implements fs.FileInfo.
func (*fileInfo) Sys() any {
	return nil
}

type StatOptions struct {
	s3.HeadObjectInput
	ctx    context.Context
	client *s3.Client
}

type StatOption interface {
	ApplyStatOption(*StatOptions)
}

type s3Reader struct {
	ctx        context.Context
	client     *s3.Client
	downloader *manager.Downloader
	input      s3.GetObjectInput
	size       int64
	offset     int64
	buf        []byte
	bufOffset  int64
}

// Close implements io.Closer.
func (f *s3Reader) Close() error {
	return nil
}

// Seek implements io.Seeker.
func (f *s3Reader) Seek(offset int64, whence int) (newOffset int64, err error) {
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

// Read implements io.Reader.
func (f *s3Reader) Read(p []byte) (n int, err error) {
	if f.offset >= f.size {
		err = io.EOF
		return
	}
	if f.buf == nil {
		n, err = f.ReadAt(p, f.offset)
		f.offset += int64(n)
		return
	}
	if f.bufOffset >= 0 && f.offset >= f.bufOffset &&
		f.offset < f.bufOffset+int64(len(f.buf)) {
		n = copy(p, f.buf[f.offset-f.bufOffset:])
		f.offset += int64(n)
		if n == len(p) {
			return
		}
	}
	var nn int
	if len(p)-n > len(f.buf) {
		nn, err = f.ReadAt(p[n:], f.offset)
		n += nn
		f.offset += int64(nn)
		copy(f.buf, p[len(p)-len(f.buf):])
		f.bufOffset = f.offset - int64(len(f.buf))
		return
	}
	if _, err = f.ReadAt(f.buf, f.offset); err != nil {
		return
	}
	f.bufOffset = f.offset
	nn = copy(p[n:], f.buf)
	f.offset += int64(nn)
	n += nn
	return
}

// ReadAt implements io.ReaderAt.
func (f *s3Reader) ReadAt(p []byte, offset int64) (n int, err error) {
	length := int64(len(p))
	if offset+length > f.size {
		length = f.size - offset
		p = p[:length]
	}
	httpRange := fmt.Sprintf("bytes=%d-%d", offset, offset+length)
	options := f.input
	options.Range = &httpRange
	if f.downloader != nil {
		var nn int64
		nn, err = f.downloader.Download(f.ctx, writerAtBuffer(p), &options)
		if isNotExist(err) {
			err = fs.ErrNotExist
		}
		n = int(nn)
		return
	}
	var output *s3.GetObjectOutput
	if output, err = f.client.GetObject(f.ctx, &options); err != nil {
		if isNotExist(err) {
			err = fs.ErrNotExist
		}
		return
	}
	defer func() {
		closeErr := output.Body.Close()
		if err == nil {
			err = closeErr
		}
	}()
	if n, err = io.ReadFull(output.Body, p); isNotExist(err) {
		err = fs.ErrNotExist
	}
	return
}

type s3Writer struct {
	ctx   context.Context
	pipeW *io.PipeWriter
	err   <-chan error
}

// Write implements io.WriteCloser
func (f *s3Writer) Write(p []byte) (n int, err error) {
	return f.pipeW.Write(p)
}

// Close implements io.WriteCloser
func (f *s3Writer) Close() error {
	return f.CloseWithError(nil)
}

func (f *s3Writer) CloseWithError(e error) error {
	closeErr := f.pipeW.CloseWithError(e)
	var err error
	select {
	case err = <-f.err:
	case <-f.ctx.Done():
		err = context.Cause(f.ctx)
	}
	if err == nil {
		err = closeErr
	}
	return err
}

var _ ReadOption = new(withContext)
var _ WriteOption = new(withContext)
var _ DeleteOption = new(withContext)
var _ StatOption = new(withContext)

type withContext struct {
	ctx context.Context
}

func (o *withContext) ApplyReadOption(opts *ReadOptions) {
	opts.ctx = o.ctx
}

func (o *withContext) ApplyWriteOption(opts *WriteOptions) {
	opts.ctx = o.ctx
}

func (o *withContext) ApplyDeleteOption(opts *DeleteOptions) {
	opts.ctx = o.ctx
}

func (o *withContext) ApplyStatOption(opts *StatOptions) {
	opts.ctx = o.ctx
}
func WithContext(ctx context.Context) *withContext {
	return &withContext{ctx}
}

var _ ReadOption = new(withClient)
var _ WriteOption = new(withClient)
var _ DeleteOption = new(withClient)
var _ StatOption = new(withClient)

type withClient struct {
	client *s3.Client
}

// ApplyStatOption implements StatOption.
func (o *withClient) ApplyStatOption(opts *StatOptions) {
	opts.client = o.client
}

func (o *withClient) ApplyReadOption(opts *ReadOptions) {
	opts.client = o.client
}

func (o *withClient) ApplyWriteOption(opts *WriteOptions) {
	opts.client = o.client
}

func (o *withClient) ApplyDeleteOption(opts *DeleteOptions) {
	opts.client = o.client
}

func WithClient(client *s3.Client) *withClient {
	return &withClient{client}
}

var _ ReadOption = new(withDownloader)

type withDownloader struct {
	downloader *manager.Downloader
}

func (o *withDownloader) ApplyReadOption(opts *ReadOptions) {
	opts.downloader = o.downloader
}

func WithDownloader(downloader *manager.Downloader) *withDownloader {
	return &withDownloader{downloader}
}

var _ ReadOption = new(withBuffer)

type withBuffer struct {
	buf []byte
}

func (o *withBuffer) ApplyReadOption(opts *ReadOptions) {
	opts.buf = o.buf
}

func WithBuffer(buf []byte) *withBuffer {
	return &withBuffer{buf}
}

var _ WriteOption = new(withUploader)

type withUploader struct {
	uploader *manager.Uploader
}

func (o *withUploader) ApplyWriteOption(opts *WriteOptions) {
	opts.uploader = o.uploader
}

func WithUploader(uploader *manager.Uploader) *withUploader {
	return &withUploader{uploader}
}

func isNotExist(err error) bool {
	if err == nil {
		return false
	}
	var responseError *awshttp.ResponseError
	return errors.As(err, &responseError) &&
		responseError.ResponseError.HTTPStatusCode() == http.StatusNotFound
}

var _ io.WriterAt = new(writerAtBuffer)

type writerAtBuffer []byte

// WriteAt implements io.WriterAt.
func (b writerAtBuffer) WriteAt(p []byte, off int64) (n int, err error) {
	copy(b[off:], p)
	// a part downloaded by a manager may extend
	// beyond the buffer for the data we originally
	// requested. to avoid failure we simply tell
	// the manager that we wrote all the data.
	n = len(p)
	return
}

var defaultClient *s3.Client

func getDefaultClient(ctx context.Context) (*s3.Client, error) {
	if defaultClient == nil {
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to load S3 SDK configuration: %w",
				err,
			)
		}
		defaultClient = s3.NewFromConfig(cfg)
	}
	return defaultClient, nil
}
