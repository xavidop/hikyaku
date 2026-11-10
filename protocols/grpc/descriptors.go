package grpc

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	// The well-known types, so a descriptor set built without
	// --include_imports still resolves them.
	_ "google.golang.org/protobuf/types/known/anypb"
	_ "google.golang.org/protobuf/types/known/durationpb"
	_ "google.golang.org/protobuf/types/known/emptypb"
	_ "google.golang.org/protobuf/types/known/fieldmaskpb"
	_ "google.golang.org/protobuf/types/known/structpb"
	_ "google.golang.org/protobuf/types/known/timestamppb"
	_ "google.golang.org/protobuf/types/known/wrapperspb"
)

// maxFiles bounds the files one descriptor set or one reflection exchange
// may bring in.
const maxFiles = 4096

// registry is a set of linked proto files: a descriptor set, or what a
// server described through reflection.
type registry struct {
	files *protoregistry.Files
	types resolver
}

// resolver finds message and extension types for protojson (google.protobuf.Any
// and extensions): the registry's own first, then those linked into hikyaku.
type resolver struct {
	local *dynamicpb.Types
}

func (r resolver) FindMessageByName(name protoreflect.FullName) (protoreflect.MessageType, error) {
	if r.local != nil {
		if t, err := r.local.FindMessageByName(name); err == nil {
			return t, nil
		}
	}
	return protoregistry.GlobalTypes.FindMessageByName(name)
}

func (r resolver) FindMessageByURL(url string) (protoreflect.MessageType, error) {
	if r.local != nil {
		if t, err := r.local.FindMessageByURL(url); err == nil {
			return t, nil
		}
	}
	return protoregistry.GlobalTypes.FindMessageByURL(url)
}

func (r resolver) FindExtensionByName(field protoreflect.FullName) (protoreflect.ExtensionType, error) {
	if r.local != nil {
		if t, err := r.local.FindExtensionByName(field); err == nil {
			return t, nil
		}
	}
	return protoregistry.GlobalTypes.FindExtensionByName(field)
}

func (r resolver) FindExtensionByNumber(message protoreflect.FullName, field protoreflect.FieldNumber) (protoreflect.ExtensionType, error) {
	if r.local != nil {
		if t, err := r.local.FindExtensionByNumber(message, field); err == nil {
			return t, nil
		}
	}
	return protoregistry.GlobalTypes.FindExtensionByNumber(message, field)
}

// files resolves descriptors from the registry first, then from the files
// linked into hikyaku (the well-known types, health, reflection, relay).
type files struct {
	local *protoregistry.Files
}

func (f files) FindFileByPath(path string) (protoreflect.FileDescriptor, error) {
	if f.local != nil {
		if d, err := f.local.FindFileByPath(path); err == nil {
			return d, nil
		}
	}
	return protoregistry.GlobalFiles.FindFileByPath(path)
}

func (f files) FindDescriptorByName(name protoreflect.FullName) (protoreflect.Descriptor, error) {
	if f.local != nil {
		if d, err := f.local.FindDescriptorByName(name); err == nil {
			return d, nil
		}
	}
	return protoregistry.GlobalFiles.FindDescriptorByName(name)
}

// loadDescriptorSet reads a FileDescriptorSet file, as written by
// protoc --include_imports --descriptor_set_out=FILE (or buf build -o FILE).
func loadDescriptorSet(path string) (*registry, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("descriptor_set: %w", err)
	}
	var set descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(b, &set); err != nil {
		return nil, fmt.Errorf("descriptor_set %s is not a FileDescriptorSet (protoc --include_imports --descriptor_set_out=%s ...): %v", path, path, err)
	}
	if len(set.GetFile()) == 0 {
		return nil, fmt.Errorf("descriptor_set %s holds no files", path)
	}
	reg, err := link(set.GetFile())
	if err != nil {
		return nil, fmt.Errorf("descriptor_set %s: %w", path, err)
	}
	return reg, nil
}

// link builds a registry from file descriptors, each after the files it
// imports. An import missing from them is looked up among the files linked
// into hikyaku, such as the well-known types.
func link(fds []*descriptorpb.FileDescriptorProto) (*registry, error) {
	if len(fds) > maxFiles {
		return nil, fmt.Errorf("%d files is more than the %d allowed", len(fds), maxFiles)
	}
	byName := make(map[string]*descriptorpb.FileDescriptorProto, len(fds))
	for _, fd := range fds {
		name := fd.GetName()
		if prev, ok := byName[name]; ok && !proto.Equal(prev, fd) {
			return nil, fmt.Errorf("%s is described twice, differently", name)
		}
		byName[name] = fd
	}
	local := new(protoregistry.Files)
	resolve := files{local: local}
	visiting := map[string]bool{}
	var visit func(name, from string) error
	visit = func(name, from string) error {
		if _, err := local.FindFileByPath(name); err == nil {
			return nil
		}
		fd, ok := byName[name]
		if !ok {
			if _, err := protoregistry.GlobalFiles.FindFileByPath(name); err == nil {
				return nil
			}
			return fmt.Errorf("%s imports %s, which is missing (build the set with --include_imports)", from, name)
		}
		if visiting[name] {
			return fmt.Errorf("%s: import cycle", name)
		}
		visiting[name] = true
		for _, dep := range fd.GetDependency() {
			if err := visit(dep, name); err != nil {
				return err
			}
		}
		f, err := protodesc.NewFile(fd, resolve)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		if err := local.RegisterFile(f); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		return nil
	}
	for _, fd := range fds {
		if err := visit(fd.GetName(), fd.GetName()); err != nil {
			return nil, err
		}
	}
	return &registry{files: local, types: resolver{local: dynamicpb.NewTypes(local)}}, nil
}

// services lists the services of the registry's own files, by full name.
func (r *registry) services() []protoreflect.ServiceDescriptor {
	var out []protoreflect.ServiceDescriptor
	r.files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		for i := 0; i < fd.Services().Len(); i++ {
			out = append(out, fd.Services().Get(i))
		}
		return true
	})
	slices.SortFunc(out, func(a, b protoreflect.ServiceDescriptor) int {
		return strings.Compare(string(a.FullName()), string(b.FullName()))
	})
	return out
}

// service finds a service by its full name.
func (r *registry) service(name string) (protoreflect.ServiceDescriptor, error) {
	d, err := r.files.FindDescriptorByName(protoreflect.FullName(name))
	if err != nil {
		if errors.Is(err, protoregistry.NotFound) {
			return nil, fmt.Errorf("service %s is not described", name)
		}
		return nil, err
	}
	sd, ok := d.(protoreflect.ServiceDescriptor)
	if !ok {
		return nil, fmt.Errorf("%s is not a service", name)
	}
	return sd, nil
}

// method finds a method by its full name, /package.Service/Method.
func (r *registry) method(full string) (protoreflect.MethodDescriptor, error) {
	svc, name, err := splitMethod(full)
	if err != nil {
		return nil, err
	}
	sd, err := r.service(svc)
	if err != nil {
		return nil, err
	}
	md := sd.Methods().ByName(protoreflect.Name(name))
	if md == nil {
		var names []string
		for i := 0; i < sd.Methods().Len(); i++ {
			names = append(names, string(sd.Methods().Get(i).Name()))
		}
		return nil, fmt.Errorf("service %s has no method %s (it has %s)", svc, name, strings.Join(names, ", "))
	}
	return md, nil
}

// splitMethod splits /package.Service/Method (the leading slash may be left
// out) into the service's full name and the method's name.
func splitMethod(full string) (service, method string, err error) {
	s := strings.TrimPrefix(strings.TrimSpace(full), "/")
	service, method, ok := strings.Cut(s, "/")
	if !ok || service == "" || method == "" || strings.Contains(method, "/") ||
		!protoreflect.FullName(service).IsValid() || !protoreflect.Name(method).IsValid() {
		return "", "", fmt.Errorf("method %q is not /package.Service/Method", full)
	}
	return service, method, nil
}

// fullMethod is a method's name as gRPC sends it: /package.Service/Method.
func fullMethod(md protoreflect.MethodDescriptor) string {
	return "/" + string(md.Parent().FullName()) + "/" + string(md.Name())
}

// streaming describes how a method streams, for error messages.
func streaming(md protoreflect.MethodDescriptor) string {
	switch {
	case md.IsStreamingClient() && md.IsStreamingServer():
		return "bidi-streaming"
	case md.IsStreamingServer():
		return "server-streaming"
	case md.IsStreamingClient():
		return "client-streaming"
	}
	return "unary"
}
