Resumable CAS gets
~~~~~~~~~~~~~~~~~~

Add resume with `ReadOffset` in the `bytestream.Read` calls to the CAS.
If the bytestream stream is terminated by network middleware.

Problem Statement
=================

Our problem
-----------

The download code uses a single connection of bytestream `Read` client
But some proxies can terminate this connection if it lives too long,
even if individual chunks are sent and received over the stream.
So we want to open a new client, and stream if it fails.

But the stream itself, where we request a `ReadOffset` is logically split from this code.
So we can not create a new stream to fetch the same resource,
to offset the read with what we already downloaded.

Life of a message and its buffer
--------------------------------

The `blob_access` interfaces specifies the `Get` function
which uses `bytestream.Read` to access blobs from the CAS.
This returns a `buffer` object,
which is generalized data
and the concrete data is typically constructed with the `ToProto` method.
That takes the underlying bytes and casts them to the desired protobuf message.

.. _process:

This is a flat list of functions that we are interested in.::

    # create a reader for a digest, it will create a stream to make sure the blob exists,
    # but not fetch the data. (Creates a buffer with lazy data fetching).
    cas_blob_access.Get
    # fetch the full buffer
    buffer.ToProto
    common conversions: toProtoViaByteSlice
    buffer.ToByteSlice (interface)
    casChunkReaderBuffer.ToByteSlice
    # loop over the stream
    common conversions: toByteSliceViaChunkReader
    ChunkReader.Read (interface)
    # single-shot
    byteStreamChunkReader.Read

    # other decorators ...
    casValidationChunkReader.MaybeFinalize
    ChunkReader.Read (interface, called again)
    ...


`toByteSliceViaChunkReader` contains the expected loop
to ask for all messages in the `Read` stream.
So `byteStreamChunkReader.Read` is a single-shot read,
and for most access patterns
this is sufficient.
The secondary call to `Read` will be addressed in the implementation,
but it is important to note that is "just works" with the unmodified code,
special code for this is a code smell.

Create a resumable chunk reader
-------------------------------

The first solution, to demonstrate viability,
is to create a new big chunk reader that resumes itself.
And leave the other components and interfaces as they are.

Then we create a configuration to enable this in `bb_clientd`
through its composition of blob access decorators.

A better long term solution is to look into how the bytestream resource name
known early in the `process`_, can be tied to the resumption
which is performed at the end of the lazy process.

We will implement a `resumableByteStreamChunkReader` in the `Implementation Log`_ section.

Preliminaries
=============

Using a patched bb-storage
--------------------------

When developing the patches it is easiest to use a patching repository,
and add `--override_repository` to `.bazelrc`.

I also have the patches on a fork, so that can be used to try it out.
But pulling from a fork is less contained than patch files.

::

    $ git clone https://github.com/stagnation/bb-storage/tree/feature/hack-resumable-bytestream-chunk-reader /tmp/resumable-bytestream-chunk-reader
    $ bazel build \
        --override_repository com_github_buildbarn_bb_storage=/tmp/resumable-bytestream-chunk-reader

.. TODO

When the patches are mature, extract them to patch files
and add them to the repository declaration.
That allows users to check out a single commit of this repo and build everything.
This is not yet done.

Start and debug
---------------

One can debug `bb_clientd` with `dlv`. I use it in the terminal directly.
These instructions do not apply any patches, see `patch bb-storage` for options.

::

    $ bazel build -c dbg //cmd/bb_clientd:bb_clientd
    $ ln -s $PWD/bazel-bin/cmd/bb_clientd/bb_clientd_/bb_clientd bb_clientd

run.sh: build and start `bb_clientd`::

    #!/bin/sh

    set -eu

    # best-effort clean up
    sudo umount ~/bb_clientd || true;
    fusermount -u ~/bb_clientd || true;

    mkdir -p \
        ~/.cache/bb_clientd/ac/persistent_state \
        ~/.cache/bb_clientd/cas/persistent_state \
        ~/.cache/bb_clientd/outputs \
        ~/bb_clientd
    OS=$(uname) bazel run -c dbg //cmd/bb_clientd "$(bazel info workspace)"/configs/bb_clientd.jsonnet

debug.sh: open `bb_clientd` in `dlv`::

    #!/bin/sh

    set -eu

    # best-effort clean up
    umount ~/bb_clientd || true;

    repo=$PWD
    # use the convenince symlink to the execroot, where all source files are spanned by the `external` tree
    # and all debug symbols map to the right files.
    cd bazel-bb-clientd || exit 1

    dlv exec "$repo"/bb_clientd "$repo"/configs/bb_clientd.jsonnet

file and line-based breakpoints work well,
as there are many implementations of the `ChunkReader` interface
using symbolic breakpoints are more tedious.

    (dlv) b cas_blob_access.go:115
    (dlv) c

Exercise the code
-----------------

You just need a valid digest to look for in the FUSE filesystem.
The best way is to pick a previous build from `bb-browser`,
and in the "Input Files" section copy-and-paste the `bb-clientd` path.
I just ran tree on it in another terminal.

Example invocation, pick a repository
and make sure `bb-clientd` is configured to work with a RBE backend::

    # start 'bb-deployments' in one terminal
    bb-deployments $ cd docker-compose
    bb-deployments $ ./run.sh

    # build 'bb-deployments' in another
    bb-deployments $ bazel build <bb-clientd flags>... //...
    bb-deployments $ tree ~/bb_clientd/cas/fuse/blobs/sha256/directory/ca2812b3f0a5b1e336644852df3eeb4b26e78fc0e84b900edbe1f760f6824891-166
    ...
        └── external
        ├── remote_config_cc
        │   └── builtin_include_directory_paths
        └── zlib
            ├── crc32.h
            ├── deflate.h
            ├── gzguts.h
            ├── inffast.h
            ├── inffixed.h
            ├── inflate.h
            ├── inftrees.h
            ├── trees.h
            ├── uncompr.c
            ├── zconf.h
            ├── zlib.h
            └── zutil.h

        10 directories, 24 files

The `tree` command is fast even with debug builds,
if you run `bb-clientd` in a debugger it will hang on your breakpoints.
The regular `bb-clientd` will not print much to its console,
but the patch will print::

    2023/08/16 15:16:39 Resumable bystream read of digest digest.Digest{value:"1-ca2812b3f0a5b1e336644852df3eeb4b26e78fc0e84b900edbe1f760f6824891-166-fuse"}.
    2023/08/16 15:16:39 Resumable bystream read of digest digest.Digest{value:"1-fb4ca3a27577f7d20bbb5ad9438fe9392f96573899d954068773c0dc1711b86c-110-fuse"}.
    2023/08/16 15:16:39 Resumable bystream read of digest digest.Digest{value:"1-fc851e227dccda68cb2773bb63909c048e402dfa9fbc20cb27cc945728673db0-77-fuse"}.
    2023/08/16 15:16:39 Resumable bystream read of digest digest.Digest{value:"1-4c76933799ae5430ffa9ebb275c4bdc669c8c0b013eec6345cb8e3488577fba8-82-fuse"}.
    ...

Note that I never saw actual resumption,
as I cannot reproduce the problem with terminated connected.

Note that the FUSE filesystem must be unmounted when restarting the process
---------------------------------------------------------------------------

If the process ever exits when debugging
it cannot simply be restarted in the debugger.
You need to unmount the FUSE filesystem outside of the process before restarting.

Implementation Log
==================

Build bb-clientd with a patched bb-storage
------------------------------------------

To start development we set up a patch repo for `bb-storage`::

    $ bazel query //external:'*' | grep bb_storage
    go_repository rule //external:com_github_buildbarn_bb_storage
    $ q-build //external:com_github_buildbarn_bb_storage
    # /home/nils/bin/gits/bb-clientd/WORKSPACE:58:16
    go_repository(
      ...
      version = "v0.0.0-20230629193729-d6a051ca744d",
    )

And we use that as a `--repository_override`.
And build `bb-clientd`, and change the CAS blob access implementation.

See the `Using a patched bb-storage`_ section for how you can use it.

Early progress
--------------

We now reach the resumable code,
and after reading the full message we try again (reading nothing).

::

    2023/08/16 13:27:40 Resumable bystream read of digest digest.Digest{value:"1-ca2812b3f0a5b1e336644852df3eeb4b26e78fc0e84b900edbe1f760f6824891-166-fuse"}.
    2023/08/16 13:27:40 Resuming bytestream read of digest digest.Digest{value:"1-ca2812b3f0a5b1e336644852df3eeb4b26e78fc0e84b900edbe1f760f6824891-166-fuse"} at offset 166.
    2023/08/16 13:27:40 Digest "1-ca2812b3f0a5b1e336644852df3eeb4b26e78fc0e84b900edbe1f760f6824891-166-fuse" is corrupted, but its storage backend does not support repairing corrupted blobs
    2023/08/16 13:27:40 rpc error: code = Internal desc = Retrying failed operation after 994.427184ms: Backend "": Failed to replicate blob 1-ca2812b3f0a5b1e336644852df3eeb4b26e78fc0e84b900edbe1f760f6824891-166-fuse: 1-ca2812b3f0a5b1e336644852df3eeb4b26e78fc0e84b900edbe1f760f6824891-166-fuse: Buffer is 0 bytes in size, while 166 bytes were expected


Furthermore, we fail to put the buffer together to something useful.

Bugs:

    - b1: We resume after a successful read.
    - b2: The buffer construction is not valid.
    - b3: We do not fetch all messages in the stream inside the stream recreation.

::

    2023/08/16 15:06:03 rpc error: code = Internal desc = Directory "1-ca2812b3f0a5b1e336644852df3eeb4b26e78fc0e84b900edbe1f760f6824891-166-fuse": Backend "": Failed to replicate blob 1-ca2812b3f0a5b1e336644852df3eeb4b26e78fc0e84b900edbe1f760f6824891-166-fuse: 1-ca2812b3f0a5b1e336644852df3eeb4b26e78fc0e84b900edbe1f760f6824891-166-fuse: Buffer is at least 332 bytes in size, while 166 bytes were expected
    2023/08/16 15:06:03 writer: Write/Writev failed, err: 22=invalid argument. opcode: READDIRPLUS

After the first pass of reading the buffer
the `casValidationChunkReader` makes a new `Read` call to make sure that we get an empty EOF result.
This is not handled, and our code returns the full buffer again.

    - b4: `maybeFinalize` finds the buffer again. (Specialization of b1)

Minimal working incision
------------------------

We now have a `resumableByteStreamChunkReader` that contains a double loop,
one to recreate the stream and retry with a read offset from what was already read.
And it has a special idempotency marker to avoid a double download in the validation.
I do not know why such code is required here, but not in the other implementations,
maybe the generated protobuf code to read from the stream has such state internally,
and our control flow can not make use of it.

The four bugs are solved.

We have a hacked version of `cas_blob_access` that lost the original functionality.

Steps forward
-------------

Create a new `resumable_cas_blob_access` implementation with the new functionality,
and restore `cas_blob_access`.
It can probably defer to `cas_blob_access` for the `Put` code,
but implement its own `Get`, and then it should not call the next decorator in the chain,
which in itself is a break from convention.

This will be damage control for this change, and may still not be good enough for upstream,
as the hack around the stream-draining is still present.

This can also be configured through the usual `jsonnet` mechanism,
As a terminal decorator for read, by pass-through for write.

Future solution paths
---------------------

We should see if `errorHandlingChunkReader` can be used.

    // newErrorHandlingChunkReader returns a ChunkReader that forwards calls
    // to a reader obtained from a Buffer. Upon I/O failure, it calls into
    // an ErrorHandler to request a new Buffer to continue the transfer.

The `ErrorHandler` should create a new bytestream stream and try to resume the download.
It already keeps track of the read offset.
