package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	reflectionv1 "google.golang.org/grpc/reflection/grpc_reflection_v1"
	reflectionv1alpha "google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

// describe asks a server, through server reflection, for the file that
// defines service and every file it imports, and links them. It speaks
// reflection v1, and v1alpha to servers that only have that.
func describe(ctx context.Context, cc grpc.ClientConnInterface, service string) (*registry, error) {
	fds, err := fetch(ctx, cc, service, openV1)
	if status.Code(err) == codes.Unimplemented {
		fds, err = fetch(ctx, cc, service, openV1Alpha)
	}
	if err != nil {
		return nil, err
	}
	return link(fds)
}

// ask sends one reflection request, for the file that defines symbol or
// the file named file, and returns the serialized files of the answer.
type ask func(symbol, file string) ([][]byte, error)

// opener opens a reflection stream of one protocol version.
type opener func(ctx context.Context, cc grpc.ClientConnInterface) (ask, error)

// fetch collects the files that describe service, following imports. An
// import the server does not have is fine when hikyaku links it (the
// well-known types).
func fetch(ctx context.Context, cc grpc.ClientConnInterface, service string, open opener) ([]*descriptorpb.FileDescriptorProto, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // ends the stream
	q, err := open(ctx, cc)
	if err != nil {
		return nil, err
	}
	var out []*descriptorpb.FileDescriptorProto
	seen := map[string]bool{}
	add := func(raw [][]byte) error {
		for _, b := range raw {
			fd := new(descriptorpb.FileDescriptorProto)
			if err := proto.Unmarshal(b, fd); err != nil {
				return fmt.Errorf("the server sent a file descriptor that does not parse: %w", err)
			}
			if seen[fd.GetName()] {
				continue
			}
			seen[fd.GetName()] = true
			out = append(out, fd)
			if len(out) > maxFiles {
				return fmt.Errorf("the server described more than %d files", maxFiles)
			}
		}
		return nil
	}
	raw, err := q(service, "")
	if err != nil {
		return nil, err
	}
	if err := add(raw); err != nil {
		return nil, err
	}
	for i := 0; i < len(out); i++ {
		for _, dep := range out[i].GetDependency() {
			if seen[dep] {
				continue
			}
			raw, err := q("", dep)
			if err != nil {
				if _, linked := protoregistry.GlobalFiles.FindFileByPath(dep); linked == nil {
					seen[dep] = true
					continue
				}
				return nil, fmt.Errorf("%s: %w", dep, err)
			}
			if err := add(raw); err != nil {
				return nil, err
			}
			seen[dep] = true
		}
	}
	return out, nil
}

func openV1(ctx context.Context, cc grpc.ClientConnInterface) (ask, error) {
	stream, err := reflectionv1.NewServerReflectionClient(cc).ServerReflectionInfo(ctx)
	if err != nil {
		return nil, err
	}
	return func(symbol, file string) ([][]byte, error) {
		req := &reflectionv1.ServerReflectionRequest{}
		if symbol != "" {
			req.MessageRequest = &reflectionv1.ServerReflectionRequest_FileContainingSymbol{FileContainingSymbol: symbol}
		} else {
			req.MessageRequest = &reflectionv1.ServerReflectionRequest_FileByFilename{FileByFilename: file}
		}
		if err := stream.Send(req); err != nil {
			return nil, streamErr(err, func() error { _, err := stream.Recv(); return err })
		}
		resp, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		switch r := resp.GetMessageResponse().(type) {
		case *reflectionv1.ServerReflectionResponse_FileDescriptorResponse:
			return r.FileDescriptorResponse.GetFileDescriptorProto(), nil
		case *reflectionv1.ServerReflectionResponse_ErrorResponse:
			return nil, status.Error(codes.Code(r.ErrorResponse.GetErrorCode()), r.ErrorResponse.GetErrorMessage())
		}
		return nil, fmt.Errorf("the server answered reflection with %T", resp.GetMessageResponse())
	}, nil
}

// openV1Alpha speaks the deprecated v1alpha protocol, which is all older
// servers (grpc-go before 1.57, grpcbin) serve.
//
//nolint:staticcheck // v1alpha is deprecated, and still what those servers speak
func openV1Alpha(ctx context.Context, cc grpc.ClientConnInterface) (ask, error) {
	stream, err := reflectionv1alpha.NewServerReflectionClient(cc).ServerReflectionInfo(ctx)
	if err != nil {
		return nil, err
	}
	return func(symbol, file string) ([][]byte, error) {
		req := &reflectionv1alpha.ServerReflectionRequest{}
		if symbol != "" {
			req.MessageRequest = &reflectionv1alpha.ServerReflectionRequest_FileContainingSymbol{FileContainingSymbol: symbol}
		} else {
			req.MessageRequest = &reflectionv1alpha.ServerReflectionRequest_FileByFilename{FileByFilename: file}
		}
		if err := stream.Send(req); err != nil {
			return nil, streamErr(err, func() error { _, err := stream.Recv(); return err })
		}
		resp, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		switch r := resp.GetMessageResponse().(type) {
		case *reflectionv1alpha.ServerReflectionResponse_FileDescriptorResponse:
			return r.FileDescriptorResponse.GetFileDescriptorProto(), nil
		case *reflectionv1alpha.ServerReflectionResponse_ErrorResponse:
			return nil, status.Error(codes.Code(r.ErrorResponse.GetErrorCode()), r.ErrorResponse.GetErrorMessage())
		}
		return nil, fmt.Errorf("the server answered reflection with %T", resp.GetMessageResponse())
	}, nil
}

// streamErr is why a send failed: io.EOF only says the stream ended, and
// the next receive says why.
func streamErr(err error, recv func() error) error {
	if !errors.Is(err, io.EOF) {
		return err
	}
	if rerr := recv(); rerr != nil && !errors.Is(rerr, io.EOF) {
		return rerr
	}
	return status.Error(codes.Unavailable, "the stream ended")
}
