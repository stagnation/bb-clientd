package virtual

import (
	"context"
	"sync"
	"syscall"

	cd_cas "github.com/buildbarn/bb-clientd/pkg/cas"
	re_cas "github.com/buildbarn/bb-remote-execution/pkg/cas"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteoutputservice"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type buildState struct {
	id                 string
	digestFunction     digest.Function
	scopeWalkerFactory *path.VirtualRootScopeWalkerFactory
}

type outputPathState struct {
	buildState     *buildState
	rootDirectory  OutputPath
	casFileFactory virtual.CASFileFactory

	// Circular linked list, used by VirtualReadDir(). By only
	// inserting new output paths at the end and ensuring that
	// cookies are monotonically increasing, we can reliably perform
	// partial reads against this directory.
	previous     *outputPathState
	next         *outputPathState
	cookie       uint64
	outputBaseID path.Component
}

// RemoteOutputServiceDirectory is FUSE directory that acts as the
// top-level directory for Remote Output Service. The Remote Output
// Service can be used by build clients to efficiently populate a
// directory with build outputs.
//
// In addition to acting as a FUSE directory, this type also implements
// a gRPC server for the Remote Output Service. This gRPC service can be
// used to start and finalize builds, but also to perform bulk creation
// and stat() operations.
//
// This implementation of the Remote Output Service is relatively
// simple:
//
//   - There is no persistency of build information across restarts.
//   - No snapshotting of completed builds takes place, meaning that only
//     the results of the latest build of a given output base are exposed.
//   - Every output path is backed by an InMemoryPrepopulatedDirectory,
//     meaning that memory usage may be high.
//   - No automatic garbage collection of old output paths is performed.
//
// This implementation should eventually be extended to address the
// issues listed above.
type RemoteOutputServiceDirectory struct {
	virtual.ReadOnlyDirectory

	handleAllocator                   virtual.StatefulHandleAllocator
	handle                            virtual.StatefulDirectoryHandle
	outputPathFactory                 OutputPathFactory
	bareContentAddressableStorage     blobstore.BlobAccess
	retryingContentAddressableStorage blobstore.BlobAccess
	directoryFetcher                  re_cas.DirectoryFetcher
	symlinkFactory                    virtual.SymlinkFactory
	maximumTreeSizeBytes              int64

	lock          sync.Mutex
	changeID      uint64
	outputBaseIDs map[path.Component]*outputPathState
	buildIDs      map[string]*outputPathState
	outputPaths   outputPathState
}

var (
	_ virtual.Directory                             = &RemoteOutputServiceDirectory{}
	_ remoteoutputservice.RemoteOutputServiceServer = &RemoteOutputServiceDirectory{}
)

// NewRemoteOutputServiceDirectory creates a new instance of
// RemoteOutputServiceDirectory.
func NewRemoteOutputServiceDirectory(handleAllocator virtual.StatefulHandleAllocator, outputPathFactory OutputPathFactory, bareContentAddressableStorage, retryingContentAddressableStorage blobstore.BlobAccess, directoryFetcher re_cas.DirectoryFetcher, symlinkFactory virtual.SymlinkFactory, maximumTreeSizeBytes int64) *RemoteOutputServiceDirectory {
	d := &RemoteOutputServiceDirectory{
		handleAllocator:                   handleAllocator,
		outputPathFactory:                 outputPathFactory,
		bareContentAddressableStorage:     bareContentAddressableStorage,
		retryingContentAddressableStorage: retryingContentAddressableStorage,
		directoryFetcher:                  directoryFetcher,
		symlinkFactory:                    symlinkFactory,
		maximumTreeSizeBytes:              maximumTreeSizeBytes,

		outputBaseIDs: map[path.Component]*outputPathState{},
		buildIDs:      map[string]*outputPathState{},
	}
	d.handle = handleAllocator.New().AsStatefulDirectory(d)
	d.outputPaths.previous = &d.outputPaths
	d.outputPaths.next = &d.outputPaths
	return d
}

// Clean all build outputs associated with a single output base.
func (d *RemoteOutputServiceDirectory) Clean(ctx context.Context, request *remoteoutputservice.CleanRequest) (*emptypb.Empty, error) {
	outputBaseID, ok := path.NewComponent(request.OutputBaseId)
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "Output base ID is not a valid filename")
	}

	d.lock.Lock()
	outputPathState, ok := d.outputBaseIDs[outputBaseID]
	d.lock.Unlock()
	if ok {
		// Remove all data stored inside the output path. This
		// must be done without holding the directory lock, as
		// NotifyRemoval() calls generated by the output path
		// could deadlock otherwise.
		if err := outputPathState.rootDirectory.RemoveAllChildren(true); err != nil {
			return nil, err
		}

		d.lock.Lock()
		if outputPathState == d.outputBaseIDs[outputBaseID] {
			delete(d.outputBaseIDs, outputBaseID)
			outputPathState.previous.next = outputPathState.next
			outputPathState.next.previous = outputPathState.previous
			d.changeID++
			if buildState := outputPathState.buildState; buildState != nil {
				delete(d.buildIDs, buildState.id)
				outputPathState.buildState = nil
			}
		}
		d.lock.Unlock()

		d.handle.NotifyRemoval(outputBaseID)
	} else if err := d.outputPathFactory.Clean(outputBaseID); err != nil {
		// This output path hasn't been accessed since startup.
		// It may be the case that there is persistent state
		// associated with this output path, so make sure that
		// is removed as well.
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// findMissingAndRemove is called during StartBuild() to remove a single
// batch of files from the output path that are no longer present in the
// Content Addressable Storage.
func (d *RemoteOutputServiceDirectory) findMissingAndRemove(ctx context.Context, queue map[digest.Digest][]func() error) error {
	set := digest.NewSetBuilder()
	for digest := range queue {
		set.Add(digest)
	}
	missing, err := d.bareContentAddressableStorage.FindMissing(ctx, set.Build())
	if err != nil {
		return util.StatusWrap(err, "Failed to find missing blobs")
	}
	for _, digest := range missing.Items() {
		for _, removeFunc := range queue[digest] {
			if err := removeFunc(); err != nil {
				return util.StatusWrapf(err, "Failed to remove file with digest %#v", digest.String())
			}
		}
	}
	return nil
}

// filterMissingChildren is called during StartBuild() to traverse over
// all files in the output path, calling FindMissingBlobs() on them to
// ensure that they will not disappear during the build. Any files that
// are missing are removed from the output path.
func (d *RemoteOutputServiceDirectory) filterMissingChildren(ctx context.Context, rootDirectory virtual.PrepopulatedDirectory, digestFunction digest.Function) error {
	queue := map[digest.Digest][]func() error{}
	var savedErr error
	if err := rootDirectory.FilterChildren(func(node virtual.InitialNode, removeFunc virtual.ChildRemover) bool {
		// Obtain the transitive closure of digests on which
		// this file or directory depends.
		var digests digest.Set
		if directory, leaf := node.GetPair(); leaf != nil {
			digests = leaf.GetContainingDigests()
		} else if digests, savedErr = directory.GetContainingDigests(ctx); savedErr != nil {
			// Can't compute the set of digests underneath
			// this directory. Remove the directory
			// entirely.
			if status.Code(savedErr) == codes.NotFound {
				savedErr = nil
				if err := removeFunc(); err != nil {
					savedErr = util.StatusWrap(err, "Failed to remove non-existent directory")
					return false
				}
				return true
			}
			return false
		}

		// Remove files that use a different instance name or
		// digest function. It may be technically valid to
		// retain these, but it comes at the cost of requiring
		// the build client to copy files between clusters, or
		// reupload them with a different hash. This may be
		// slower than requiring a rebuild.
		for _, blobDigest := range digests.Items() {
			if !blobDigest.UsesDigestFunction(digestFunction) {
				if err := removeFunc(); err != nil {
					savedErr = util.StatusWrapf(err, "Failed to remove file with different instance name or digest function with digest %#v", blobDigest.String())
					return false
				}
				return true
			}
		}

		for _, blobDigest := range digests.Items() {
			if len(queue) >= blobstore.RecommendedFindMissingDigestsCount {
				// Maximum number of digests reached.
				savedErr = d.findMissingAndRemove(ctx, queue)
				if savedErr != nil {
					return false
				}
				queue = map[digest.Digest][]func() error{}
			}
			queue[blobDigest] = append(queue[blobDigest], removeFunc)
		}
		return true
	}); err != nil {
		return err
	}
	if savedErr != nil {
		return savedErr
	}

	// Process the final batch of files.
	if len(queue) > 0 {
		return d.findMissingAndRemove(ctx, queue)
	}
	return nil
}

// StartBuild is called by a build client to indicate that a new build
// in a given output base is starting.
func (d *RemoteOutputServiceDirectory) StartBuild(ctx context.Context, request *remoteoutputservice.StartBuildRequest) (*remoteoutputservice.StartBuildResponse, error) {
	// Compute the full output path and the output path suffix. The
	// former needs to be used by us, while the latter is
	// communicated back to the client.
	outputPath, scopeWalker := path.EmptyBuilder.Join(path.NewAbsoluteScopeWalker(path.VoidComponentWalker))
	if err := path.Resolve(request.OutputPathPrefix, scopeWalker); err != nil {
		return nil, util.StatusWrap(err, "Failed to resolve output path prefix")
	}
	outputPathSuffix, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
	outputPath, scopeWalker = outputPath.Join(scopeWalker)
	outputBaseID, ok := path.NewComponent(request.OutputBaseId)
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "Output base ID is not a valid filename")
	}
	componentWalker, err := scopeWalker.OnScope(false)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to resolve output path")
	}
	if _, err := componentWalker.OnTerminal(outputBaseID); err != nil {
		return nil, util.StatusWrap(err, "Failed to resolve output path")
	}

	// Create a virtual root based on the output path and provided
	// aliases. This will be used to properly resolve targets of
	// symbolic links stored in the output path.
	scopeWalkerFactory, err := path.NewVirtualRootScopeWalkerFactory(outputPath.String(), request.OutputPathAliases)
	if err != nil {
		return nil, err
	}

	instanceName, err := digest.NewInstanceName(request.InstanceName)
	if err != nil {
		return nil, util.StatusWrapf(err, "Failed to parse instance name %#v", request.InstanceName)
	}
	digestFunction, err := instanceName.GetDigestFunction(request.DigestFunction, 0)
	if err != nil {
		return nil, err
	}

	d.lock.Lock()
	state, ok := d.buildIDs[request.BuildId]
	if !ok {
		state, ok = d.outputBaseIDs[outputBaseID]
		if ok {
			if buildState := state.buildState; buildState != nil {
				// A previous build is running that wasn't
				// finalized properly. Forcefully finalize it.
				delete(d.buildIDs, buildState.id)
				state.buildState = nil
			}
		} else {
			// No previous builds have been run for this
			// output base. Create a new output path.
			//
			// TODO: This should not use DefaultErrorLogger.
			// Instead, we should capture errors, so that we
			// can propagate them back to the build client.
			// This allows the client to retry, or at least
			// display the error immediately, so that users
			// don't need to check logs.
			errorLogger := util.DefaultErrorLogger
			casFileFactory := virtual.NewStatelessHandleAllocatingCASFileFactory(
				virtual.NewBlobAccessCASFileFactory(
					context.Background(),
					d.retryingContentAddressableStorage,
					errorLogger),
				d.handleAllocator.New())
			state = &outputPathState{
				rootDirectory:  d.outputPathFactory.StartInitialBuild(outputBaseID, casFileFactory, digestFunction, errorLogger),
				casFileFactory: casFileFactory,

				previous:     d.outputPaths.previous,
				next:         &d.outputPaths,
				cookie:       d.changeID,
				outputBaseID: outputBaseID,
			}
			d.outputBaseIDs[outputBaseID] = state
			state.previous.next = state
			state.next.previous = state
			d.changeID++
		}

		// Allow BatchCreate() and BatchStat() requests for the
		// new build ID.
		state.buildState = &buildState{
			id:                 request.BuildId,
			digestFunction:     digestFunction,
			scopeWalkerFactory: scopeWalkerFactory,
		}
		d.buildIDs[request.BuildId] = state
	}
	d.lock.Unlock()

	// Call ContentAddressableStorage.FindMissingBlobs() on all of
	// the files and tree objects contained within the output path,
	// so that we have the certainty that they don't disappear
	// during the build. Remove all of the files and directories
	// that are missing, so that the client can detect their absence
	// and rebuild them.
	if err := d.filterMissingChildren(ctx, state.rootDirectory, digestFunction); err != nil {
		return nil, util.StatusWrap(err, "Failed to filter contents of the output path")
	}

	return &remoteoutputservice.StartBuildResponse{
		// TODO: Fill in InitialOutputPathContents, so that the
		// client can skip parts of its analysis. The easiest
		// way to achieve this would be to freeze the contents
		// of the output path between builds.
		OutputPathSuffix: outputPathSuffix.String(),
	}, nil
}

// getOutputPathAndBuildState returns the state objects associated with
// a given build ID. This function is used by all gRPC methods that can
// only be invoked as part of a build (e.g., BatchCreate(), BatchStat()).
func (d *RemoteOutputServiceDirectory) getOutputPathAndBuildState(buildID string) (*outputPathState, *buildState, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	outputPathState, ok := d.buildIDs[buildID]
	if !ok {
		return nil, nil, status.Error(codes.FailedPrecondition, "Build ID is not associated with any running build")
	}
	return outputPathState, outputPathState.buildState, nil
}

// directoryCreatingComponentWalker is an implementation of
// ComponentWalker that is used by BatchCreate() to resolve the path
// prefix under which all provided files, symbolic links and directories
// should be created.
//
// This resolver forcefully creates all intermediate pathname
// components, removing any non-directories that are in the way.
type directoryCreatingComponentWalker struct {
	stack util.NonEmptyStack[virtual.PrepopulatedDirectory]
}

func (cw *directoryCreatingComponentWalker) OnDirectory(name path.Component) (path.GotDirectoryOrSymlink, error) {
	child, err := cw.stack.Peek().CreateAndEnterPrepopulatedDirectory(name)
	if err != nil {
		return nil, err
	}
	cw.stack.Push(child)
	return path.GotDirectory{
		Child:        cw,
		IsReversible: true,
	}, nil
}

func (cw *directoryCreatingComponentWalker) OnTerminal(name path.Component) (*path.GotSymlink, error) {
	return path.OnTerminalViaOnDirectory(cw, name)
}

func (cw *directoryCreatingComponentWalker) OnUp() (path.ComponentWalker, error) {
	if _, ok := cw.stack.PopSingle(); !ok {
		return nil, status.Error(codes.InvalidArgument, "Path resolves to a location outside the output path")
	}
	return cw, nil
}

func (cw *directoryCreatingComponentWalker) createChild(outputPath string, initialNode virtual.InitialNode) error {
	outputParentCreator := parentDirectoryCreatingComponentWalker{
		stack: cw.stack.Copy(),
	}
	if err := path.Resolve(outputPath, path.NewRelativeScopeWalker(&outputParentCreator)); err != nil {
		return util.StatusWrap(err, "Failed to resolve path")
	}
	name := outputParentCreator.TerminalName
	if name == nil {
		return status.Errorf(codes.InvalidArgument, "Path resolves to a directory")
	}
	return outputParentCreator.stack.Peek().CreateChildren(
		map[path.Component]virtual.InitialNode{
			*name: initialNode,
		},
		true)
}

// parentDirectoryCreatingComponentWalker is an implementation of
// ComponentWalker that is used by BatchCreate() to resolve the parent
// directory of the path where a file, directory or symlink needs to be
// created.
type parentDirectoryCreatingComponentWalker struct {
	path.TerminalNameTrackingComponentWalker
	stack util.NonEmptyStack[virtual.PrepopulatedDirectory]
}

func (cw *parentDirectoryCreatingComponentWalker) OnDirectory(name path.Component) (path.GotDirectoryOrSymlink, error) {
	child, err := cw.stack.Peek().CreateAndEnterPrepopulatedDirectory(name)
	if err != nil {
		return nil, err
	}
	cw.stack.Push(child)
	return path.GotDirectory{
		Child:        cw,
		IsReversible: true,
	}, nil
}

func (cw *parentDirectoryCreatingComponentWalker) OnUp() (path.ComponentWalker, error) {
	if _, ok := cw.stack.PopSingle(); !ok {
		return nil, status.Error(codes.InvalidArgument, "Path resolves to a location outside the output path")
	}
	return cw, nil
}

// BatchCreate can be called by a build client to create files, symbolic
// links and directories.
//
// Because files and directories are provided in the form of OutputFile
// and OutputDirectory messages, this implementation is capable of
// creating files and directories whose contents get loaded from the
// Content Addressable Storage lazily.
func (d *RemoteOutputServiceDirectory) BatchCreate(ctx context.Context, request *remoteoutputservice.BatchCreateRequest) (*emptypb.Empty, error) {
	outputPathState, buildState, err := d.getOutputPathAndBuildState(request.BuildId)
	if err != nil {
		return nil, err
	}

	// Resolve the path prefix. Optionally, remove all of its contents.
	prefixCreator := directoryCreatingComponentWalker{
		stack: util.NewNonEmptyStack[virtual.PrepopulatedDirectory](outputPathState.rootDirectory),
	}
	if err := path.Resolve(request.PathPrefix, path.NewRelativeScopeWalker(&prefixCreator)); err != nil {
		return nil, util.StatusWrap(err, "Failed to create path prefix directory")
	}
	if request.CleanPathPrefix {
		if err := prefixCreator.stack.Peek().RemoveAllChildren(false); err != nil {
			return nil, util.StatusWrap(err, "Failed to clean path prefix directory")
		}
	}

	// Create requested files.
	for _, entry := range request.Files {
		childDigest, err := buildState.digestFunction.NewDigestFromProto(entry.Digest)
		if err != nil {
			return nil, util.StatusWrapf(err, "Invalid digest for file %#v", entry.Path)
		}
		leaf := outputPathState.casFileFactory.LookupFile(childDigest, entry.IsExecutable)
		if err := prefixCreator.createChild(entry.Path, virtual.InitialNode{}.FromLeaf(leaf)); err != nil {
			leaf.Unlink()
			return nil, util.StatusWrapf(err, "Failed to create file %#v", entry.Path)
		}
	}

	// Create requested directories.
	for _, entry := range request.Directories {
		childDigest, err := buildState.digestFunction.NewDigestFromProto(entry.TreeDigest)
		if err != nil {
			return nil, util.StatusWrapf(err, "Invalid digest for directory %#v", entry.Path)
		}
		if sizeBytes := childDigest.GetSizeBytes(); sizeBytes > d.maximumTreeSizeBytes {
			return nil, status.Errorf(codes.InvalidArgument, "Directory %#v is %d bytes in size, which exceeds the permitted maximum of %d bytes", entry.Path, sizeBytes, d.maximumTreeSizeBytes)
		}
		if err := prefixCreator.createChild(
			entry.Path,
			virtual.InitialNode{}.FromDirectory(
				virtual.NewCASInitialContentsFetcher(
					context.Background(),
					cd_cas.NewTreeDirectoryWalker(d.directoryFetcher, childDigest),
					outputPathState.casFileFactory,
					d.symlinkFactory,
					buildState.digestFunction))); err != nil {
			return nil, util.StatusWrapf(err, "Failed to create directory %#v", entry.Path)
		}
	}

	// Create requested symbolic links.
	for _, entry := range request.Symlinks {
		leaf := d.symlinkFactory.LookupSymlink([]byte(entry.Target))
		if err := prefixCreator.createChild(entry.Path, virtual.InitialNode{}.FromLeaf(leaf)); err != nil {
			leaf.Unlink()
			return nil, util.StatusWrapf(err, "Failed to create symbolic link %#v", entry.Path)
		}
	}

	return &emptypb.Empty{}, nil
}

// statWalker is an implementation of ScopeWalker and ComponentWalker
// that is used by BatchStat() to resolve the file or directory
// corresponding to a requested path. It is capable of expanding
// symbolic links, if encountered.
type statWalker struct {
	followSymlinks bool
	digestFunction *digest.Function

	stack      util.NonEmptyStack[virtual.PrepopulatedDirectory]
	fileStatus *remoteoutputservice.FileStatus
}

func (cw *statWalker) OnScope(absolute bool) (path.ComponentWalker, error) {
	if absolute {
		cw.stack.PopAll()
	}
	// Currently in a known directory.
	cw.fileStatus = &remoteoutputservice.FileStatus{
		FileType: &remoteoutputservice.FileStatus_Directory_{},
	}
	return cw, nil
}

func (cw *statWalker) OnDirectory(name path.Component) (path.GotDirectoryOrSymlink, error) {
	child, err := cw.stack.Peek().LookupChild(name)
	if err != nil {
		return nil, err
	}

	directory, leaf := child.GetPair()
	if directory != nil {
		// Got a directory.
		cw.stack.Push(directory)
		return path.GotDirectory{
			Child:        cw,
			IsReversible: true,
		}, nil
	}

	target, err := leaf.Readlink()
	if err == syscall.EINVAL {
		return nil, syscall.ENOTDIR
	} else if err != nil {
		return nil, err
	}

	// Got a symbolic link in the middle of a path. Those should
	// always be followed.
	cw.fileStatus = &remoteoutputservice.FileStatus{
		FileType: &remoteoutputservice.FileStatus_External_{},
	}
	return path.GotSymlink{
		Parent: cw,
		Target: target,
	}, nil
}

func (cw *statWalker) OnTerminal(name path.Component) (*path.GotSymlink, error) {
	child, err := cw.stack.Peek().LookupChild(name)
	if err != nil {
		return nil, err
	}

	directory, leaf := child.GetPair()
	if directory != nil {
		// Got a directory. The existing FileStatus is sufficient.
		cw.stack.Push(directory)
		return nil, nil
	}

	if cw.followSymlinks {
		target, err := leaf.Readlink()
		if err == nil {
			// Got a symbolic link, and we should follow it.
			cw.fileStatus = &remoteoutputservice.FileStatus{
				FileType: &remoteoutputservice.FileStatus_External_{},
			}
			return &path.GotSymlink{
				Parent: cw,
				Target: target,
			}, nil
		}
		if err != syscall.EINVAL {
			return nil, err
		}
	}

	fileStatus, err := leaf.GetOutputServiceFileStatus(cw.digestFunction)
	if err != nil {
		return nil, err
	}
	cw.fileStatus = fileStatus
	return nil, nil
}

func (cw *statWalker) OnUp() (path.ComponentWalker, error) {
	if _, ok := cw.stack.PopSingle(); !ok {
		cw.fileStatus = &remoteoutputservice.FileStatus{
			FileType: &remoteoutputservice.FileStatus_External_{},
		}
		return path.VoidComponentWalker, nil
	}
	return cw, nil
}

// BatchStat can be called by a build client to obtain the status of
// files and directories.
//
// Calling this method over gRPC may be far more efficient than
// obtaining this information through the FUSE file system, as batching
// significantly reduces the amount of context switching. It also
// prevents the computation of digests for files for which the digest is
// already known.
func (d *RemoteOutputServiceDirectory) BatchStat(ctx context.Context, request *remoteoutputservice.BatchStatRequest) (*remoteoutputservice.BatchStatResponse, error) {
	outputPathState, buildState, err := d.getOutputPathAndBuildState(request.BuildId)
	if err != nil {
		return nil, err
	}

	response := remoteoutputservice.BatchStatResponse{
		Responses: make([]*remoteoutputservice.StatResponse, 0, len(request.Paths)),
	}
	for _, statPath := range request.Paths {
		statWalker := statWalker{
			followSymlinks: request.FollowSymlinks,
			stack:          util.NewNonEmptyStack[virtual.PrepopulatedDirectory](outputPathState.rootDirectory),
			fileStatus: &remoteoutputservice.FileStatus{
				FileType: &remoteoutputservice.FileStatus_External_{},
			},
		}
		if request.IncludeFileDigest {
			statWalker.digestFunction = &buildState.digestFunction
		}

		resolvedPath, scopeWalker := path.EmptyBuilder.Join(
			buildState.scopeWalkerFactory.New(path.NewLoopDetectingScopeWalker(&statWalker)))
		if err := path.Resolve(statPath, scopeWalker); err == syscall.ENOENT {
			// Path does not exist.
			response.Responses = append(response.Responses, &remoteoutputservice.StatResponse{})
		} else if err != nil {
			// Some other error occurred.
			return nil, util.StatusWrapf(err, "Failed to resolve path %#v beyond %#v", statPath, resolvedPath.String())
		} else {
			switch fileType := statWalker.fileStatus.FileType.(type) {
			case *remoteoutputservice.FileStatus_Directory_:
				// For directories we need to provide the last
				// modification time, as the client uses that to
				// invalidate cached results.
				var attributes virtual.Attributes
				statWalker.stack.Peek().VirtualGetAttributes(ctx, virtual.AttributesMaskLastDataModificationTime, &attributes)
				lastModifiedTime, ok := attributes.GetLastDataModificationTime()
				if !ok {
					panic("Directory did not provide a last data modification time, even though the Remote Output Service protocol requires it")
				}
				fileType.Directory = &remoteoutputservice.FileStatus_Directory{
					LastModifiedTime: timestamppb.New(lastModifiedTime),
				}
			case *remoteoutputservice.FileStatus_External_:
				// Path resolves to a location outside the file
				// system. Return the resolved path back to the
				// client, so it can stat() it manually.
				fileType.External = &remoteoutputservice.FileStatus_External{
					NextPath: resolvedPath.String(),
				}
			}
			response.Responses = append(response.Responses, &remoteoutputservice.StatResponse{
				FileStatus: statWalker.fileStatus,
			})
		}
	}
	return &response, nil
}

// FinalizeBuild can be called by a build client to indicate the current
// build has completed. This prevents successive BatchCreate() and
// BatchStat() calls from being processed.
func (d *RemoteOutputServiceDirectory) FinalizeBuild(ctx context.Context, request *remoteoutputservice.FinalizeBuildRequest) (*emptypb.Empty, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	// Silently ignore requests for unknown build IDs. This ensures
	// that FinalizeBuild() remains idempotent.
	if outputPathState, ok := d.buildIDs[request.BuildId]; ok {
		buildState := outputPathState.buildState
		outputPathState.rootDirectory.FinalizeBuild(ctx, buildState.digestFunction)
		delete(d.buildIDs, buildState.id)
		outputPathState.buildState = nil
	}
	return &emptypb.Empty{}, nil
}

// VirtualGetAttributes returns the attributes of the root directory of
// the Remote Output Service.
func (d *RemoteOutputServiceDirectory) VirtualGetAttributes(ctx context.Context, requested virtual.AttributesMask, attributes *virtual.Attributes) {
	attributes.SetFileType(filesystem.FileTypeDirectory)
	attributes.SetPermissions(virtual.PermissionsRead | virtual.PermissionsExecute)
	attributes.SetSizeBytes(0)
	if requested&(virtual.AttributesMaskChangeID|virtual.AttributesMaskLinkCount) != 0 {
		d.lock.Lock()
		attributes.SetChangeID(d.changeID)
		attributes.SetLinkCount(virtual.EmptyDirectoryLinkCount + uint32(len(d.outputBaseIDs)))
		d.lock.Unlock()
	}
	d.handle.GetAttributes(requested, attributes)
}

// VirtualLookup can be used to look up the root directory of an output
// path for a given output base.
func (d *RemoteOutputServiceDirectory) VirtualLookup(ctx context.Context, name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.DirectoryChild, virtual.Status) {
	d.lock.Lock()
	outputPathState, ok := d.outputBaseIDs[name]
	d.lock.Unlock()
	if !ok {
		return virtual.DirectoryChild{}, virtual.StatusErrNoEnt
	}
	outputPathState.rootDirectory.VirtualGetAttributes(ctx, requested, out)
	return virtual.DirectoryChild{}.FromDirectory(outputPathState.rootDirectory), virtual.StatusOK
}

// VirtualOpenChild can be used to open or create a file in the root
// directory of the Remote Output Service. Because this directory never
// contains any files, this function is guaranteed to fail.
func (d *RemoteOutputServiceDirectory) VirtualOpenChild(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, openedFileAttributes *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
	d.lock.Lock()
	_, ok := d.outputBaseIDs[name]
	d.lock.Unlock()
	if ok {
		return virtual.ReadOnlyDirectoryOpenChildWrongFileType(existingOptions, virtual.StatusErrIsDir)
	}
	return virtual.ReadOnlyDirectoryOpenChildDoesntExist(createAttributes)
}

// VirtualReadDir returns a list of all the output paths managed by this
// Remote Output Service.
func (d *RemoteOutputServiceDirectory) VirtualReadDir(ctx context.Context, firstCookie uint64, requested virtual.AttributesMask, reporter virtual.DirectoryEntryReporter) virtual.Status {
	d.lock.Lock()
	defer d.lock.Unlock()

	// Find the first output path past the provided cookie.
	outputPathState := d.outputPaths.next
	for {
		if outputPathState == &d.outputPaths {
			return virtual.StatusOK
		}
		if outputPathState.cookie >= firstCookie {
			break
		}
		outputPathState = outputPathState.next
	}

	// Return information for the remaining output paths.
	for ; outputPathState != &d.outputPaths; outputPathState = outputPathState.next {
		child := outputPathState.rootDirectory
		var attributes virtual.Attributes
		child.VirtualGetAttributes(ctx, requested, &attributes)
		if !reporter.ReportEntry(outputPathState.cookie+1, outputPathState.outputBaseID, virtual.DirectoryChild{}.FromDirectory(child), &attributes) {
			break
		}
	}
	return virtual.StatusOK
}
