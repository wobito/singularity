// Copyright (c) 2022-2023, Sylabs Inc. All rights reserved.
// This software is licensed under a 3-clause BSD license. Please consult the
// LICENSE.md file distributed with the sources of this project regarding your
// rights to use or distribute this software.

// Package oci implements a Launcher that will configure and launch a container
// with an OCI runtime. It also provides implementations of OCI state
// transitions that can be called directly, Create/Start/Kill etc.
package oci

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/sylabs/singularity/internal/pkg/buildcfg"
	"github.com/sylabs/singularity/internal/pkg/cache"
	"github.com/sylabs/singularity/internal/pkg/cgroups"
	"github.com/sylabs/singularity/internal/pkg/runtime/launcher"
	"github.com/sylabs/singularity/internal/pkg/util/fs/files"
	"github.com/sylabs/singularity/internal/pkg/util/user"
	"github.com/sylabs/singularity/pkg/ocibundle"
	"github.com/sylabs/singularity/pkg/ocibundle/native"
	"github.com/sylabs/singularity/pkg/ocibundle/tools"
	"github.com/sylabs/singularity/pkg/sylog"
	"github.com/sylabs/singularity/pkg/util/singularityconf"
)

var (
	ErrUnsupportedOption = errors.New("not supported by OCI launcher")
	ErrNotImplemented    = errors.New("not implemented by OCI launcher")
)

// Launcher will holds configuration for, and will launch a container using an
// OCI runtime.
type Launcher struct {
	cfg             launcher.Options
	singularityConf *singularityconf.File
}

// NewLauncher returns a oci.Launcher with an initial configuration set by opts.
func NewLauncher(opts ...launcher.Option) (*Launcher, error) {
	lo := launcher.Options{}
	for _, opt := range opts {
		if err := opt(&lo); err != nil {
			return nil, fmt.Errorf("%w", err)
		}
	}

	if err := checkOpts(lo); err != nil {
		return nil, err
	}

	c := singularityconf.GetCurrentConfig()
	if c == nil {
		return nil, fmt.Errorf("singularity configuration is not initialized")
	}

	return &Launcher{cfg: lo, singularityConf: c}, nil
}

// checkOpts ensures that options set are supported by the oci.Launcher.
//
// nolint:maintidx
func checkOpts(lo launcher.Options) error {
	badOpt := []string{}

	if lo.Writable {
		badOpt = append(badOpt, "Writable")
	}
	if lo.WritableTmpfs {
		badOpt = append(badOpt, "WritableTmpfs")
	}
	if len(lo.OverlayPaths) > 0 {
		badOpt = append(badOpt, "OverlayPaths")
	}
	if len(lo.ScratchDirs) > 0 {
		badOpt = append(badOpt, "ScratchDirs")
	}
	if lo.WorkDir != "" {
		badOpt = append(badOpt, "WorkDir")
	}

	// Home is always sent from the CLI, and must be valid as an option, but
	// CustomHome signifies if it was a user specified value which we don't
	// support (yet).
	if lo.CustomHome {
		badOpt = append(badOpt, "CustomHome")
	}
	if lo.NoHome {
		badOpt = append(badOpt, "NoHome")
	}

	if len(lo.FuseMount) > 0 {
		badOpt = append(badOpt, "FuseMount")
	}

	if len(lo.NoMount) > 0 {
		badOpt = append(badOpt, "NoMount")
	}

	if lo.NvCCLI {
		badOpt = append(badOpt, "NvCCLI")
	}

	if len(lo.ContainLibs) > 0 {
		badOpt = append(badOpt, "ContainLibs")
	}
	if lo.Proot != "" {
		badOpt = append(badOpt, "Proot")
	}

	if lo.CleanEnv {
		badOpt = append(badOpt, "CleanEnv")
	}
	if lo.NoEval {
		badOpt = append(badOpt, "NoEval")
	}

	// Network always set in CLI layer even if network namespace not requested.
	// We only support isolation at present
	if lo.Namespaces.Net && lo.Network != "none" {
		badOpt = append(badOpt, "Network (except none)")
	}

	if len(lo.NetworkArgs) > 0 {
		badOpt = append(badOpt, "NetworkArgs")
	}
	if lo.Hostname != "" {
		badOpt = append(badOpt, "Hostname")
	}
	if lo.DNS != "" {
		badOpt = append(badOpt, "DNS")
	}

	if lo.AddCaps != "" {
		badOpt = append(badOpt, "AddCaps")
	}
	if lo.DropCaps != "" {
		badOpt = append(badOpt, "DropCaps")
	}
	if lo.AllowSUID {
		badOpt = append(badOpt, "AllowSUID")
	}
	if lo.KeepPrivs {
		badOpt = append(badOpt, "KeepPrivs")
	}
	if lo.NoPrivs {
		badOpt = append(badOpt, "NoPrivs")
	}
	if len(lo.SecurityOpts) > 0 {
		badOpt = append(badOpt, "SecurityOpts")
	}
	if lo.NoUmask {
		badOpt = append(badOpt, "NoUmask")
	}

	// ConfigFile always set by CLI. We should support only the default from build time.
	if lo.ConfigFile != "" && lo.ConfigFile != buildcfg.SINGULARITY_CONF_FILE {
		badOpt = append(badOpt, "ConfigFile")
	}

	if lo.ShellPath != "" {
		badOpt = append(badOpt, "ShellPath")
	}
	if lo.PwdPath != "" {
		badOpt = append(badOpt, "PwdPath")
	}

	if lo.Boot {
		badOpt = append(badOpt, "Boot")
	}
	if lo.NoInit {
		badOpt = append(badOpt, "NoInit")
	}
	if lo.Contain {
		badOpt = append(badOpt, "Contain")
	}
	if lo.ContainAll {
		badOpt = append(badOpt, "ContainAll")
	}

	if lo.AppName != "" {
		badOpt = append(badOpt, "AppName")
	}

	if lo.KeyInfo != nil {
		badOpt = append(badOpt, "KeyInfo")
	}

	if lo.SIFFUSE {
		badOpt = append(badOpt, "SIFFUSE")
	}

	if len(badOpt) > 0 {
		return fmt.Errorf("%w: %s", ErrUnsupportedOption, strings.Join(badOpt, ","))
	}

	return nil
}

// createSpec creates an initial OCI runtime specification, suitable to launch a
// container. This spec excludes the Process config, as this has to be computed
// where the image config is available, to account for the image's CMD /
// ENTRYPOINT / ENV / USER.
func (l *Launcher) createSpec() (*specs.Spec, error) {
	spec := minimalSpec()

	spec = addNamespaces(spec, l.cfg.Namespaces)

	mounts, err := l.getMounts()
	if err != nil {
		return nil, err
	}
	spec.Mounts = mounts

	cgPath, resources, err := l.getCgroup()
	if err != nil {
		return nil, err
	}
	if cgPath != "" {
		spec.Linux.CgroupsPath = cgPath
		spec.Linux.Resources = resources
	}

	return &spec, nil
}

// finalizeSpec updates the bundle config, filling in Process config that depends on the image spec.
func (l *Launcher) finalizeSpec(ctx context.Context, b ocibundle.Bundle, spec *specs.Spec, image string, process string, args []string) (err error) {
	imgSpec := b.ImageSpec()
	if imgSpec == nil {
		return fmt.Errorf("bundle has no image spec")
	}

	// In the absence of a USER in the OCI image config, we will run the
	// container process as our current user / group.
	currentUID := uint32(os.Getuid())
	currentGID := uint32(os.Getgid())
	targetUID := currentUID
	targetGID := currentGID
	containerUser := false

	// If the OCI image config specifies a USER we will:
	//  * When unprivileged - run as that user, via nested subuid/gid mappings (host user -> userns root -> OCI USER)
	//  * When privileged - directly run as that user, as a host uid/gid.
	if imgSpec.Config.User != "" {
		imgUser, err := tools.BundleUser(b.Path(), imgSpec.Config.User)
		if err != nil {
			return err
		}
		imgUID, err := strconv.ParseUint(imgUser.Uid, 10, 32)
		if err != nil {
			return err
		}
		imgGID, err := strconv.ParseUint(imgUser.Gid, 10, 32)
		if err != nil {
			return err
		}
		targetUID = uint32(imgUID)
		targetGID = uint32(imgGID)
		containerUser = true
		sylog.Debugf("Running as USER specified in OCI image config %d:%d", targetUID, targetGID)
	}

	// Fakeroot always overrides to give us root in the container (via userns & idmap if unprivileged).
	if l.cfg.Fakeroot {
		targetUID = 0
		targetGID = 0
	}

	if targetUID != 0 && currentUID != 0 {
		uidMap, gidMap, err := l.getReverseUserMaps(targetUID, targetGID)
		if err != nil {
			return err
		}
		spec.Linux.UIDMappings = uidMap
		spec.Linux.GIDMappings = gidMap
	}

	u := specs.User{
		UID: targetUID,
		GID: targetGID,
	}

	specProcess, err := l.getProcess(ctx, *imgSpec, image, b.Path(), process, args, u)
	if err != nil {
		return err
	}
	spec.Process = specProcess
	if err := b.Update(ctx, spec); err != nil {
		return err
	}

	// If we are entering as root, or a USER defined in the container, then passwd/group
	// information should be present already.
	if targetUID == 0 || containerUser {
		return nil
	}
	// Otherewise, add to the passwd and group files in the container.
	if err := l.updatePasswdGroup(tools.RootFs(b.Path()).Path(), targetUID, targetGID); err != nil {
		return err
	}
	return nil
}

func (l *Launcher) updatePasswdGroup(rootfs string, uid, gid uint32) error {
	if os.Getuid() == 0 || l.cfg.Fakeroot {
		return nil
	}

	containerPasswd := filepath.Join(rootfs, "etc", "passwd")
	containerGroup := filepath.Join(rootfs, "etc", "group")

	pw, err := user.CurrentOriginal()
	if err != nil {
		return err
	}

	sylog.Debugf("Updating passwd file: %s", containerPasswd)
	content, err := files.Passwd(containerPasswd, pw.Dir, int(uid))
	if err != nil {
		sylog.Warningf("%s", err)
	} else if err := os.WriteFile(containerPasswd, content, 0o755); err != nil {
		return fmt.Errorf("while writing passwd file: %w", err)
	}

	sylog.Debugf("Updating group file: %s", containerGroup)
	content, err = files.Group(containerGroup, int(uid), []int{int(gid)})
	if err != nil {
		sylog.Warningf("%s", err)
	} else if err := os.WriteFile(containerGroup, content, 0o755); err != nil {
		return fmt.Errorf("while writing passwd file: %w", err)
	}

	return nil
}

// Exec will interactively execute a container via the runc low-level runtime.
// image is a reference to an OCI image, e.g. docker://ubuntu or oci:/tmp/mycontainer
func (l *Launcher) Exec(ctx context.Context, image string, process string, args []string, instanceName string) error {
	if instanceName != "" {
		return fmt.Errorf("%w: instanceName", ErrNotImplemented)
	}

	if l.cfg.SysContext == nil {
		return fmt.Errorf("launcher SysContext must be set for OCI image handling")
	}

	bundleDir, err := os.MkdirTemp("", "oci-bundle")
	if err != nil {
		return nil
	}
	defer func() {
		sylog.Debugf("Removing OCI bundle at: %s", bundleDir)
		if err := os.RemoveAll(bundleDir); err != nil {
			sylog.Errorf("Couldn't remove OCI bundle %s: %v", bundleDir, err)
		}
	}()

	sylog.Debugf("Creating OCI bundle at: %s", bundleDir)

	var imgCache *cache.Handle
	if !l.cfg.CacheDisabled {
		imgCache, err = cache.New(cache.Config{
			ParentDir: os.Getenv(cache.DirEnv),
		})
		if err != nil {
			return err
		}
	}

	// Create OCI runtime spec, excluding the Process settings which must consider the image spec.
	spec, err := l.createSpec()
	if err != nil {
		return fmt.Errorf("while creating OCI spec: %w", err)
	}

	// Create a bundle - obtain and extract the image.
	b, err := native.New(
		native.OptBundlePath(bundleDir),
		native.OptImageRef(image),
		native.OptSysCtx(l.cfg.SysContext),
		native.OptImgCache(imgCache),
	)
	if err != nil {
		return err
	}
	if err := b.Create(ctx, spec); err != nil {
		return err
	}

	// With reference to the bundle's image spec, now set the process configuration.
	if err := l.finalizeSpec(ctx, b, spec, image, process, args); err != nil {
		return err
	}

	id, err := uuid.NewRandom()
	if err != nil {
		return fmt.Errorf("while generating container id: %w", err)
	}

	if os.Getuid() == 0 {
		// Direct execution of runc/crun run.
		err = Run(ctx, id.String(), b.Path(), "", l.singularityConf.SystemdCgroups)
	} else {
		// Reexec singularity oci run in a userns with mappings.
		// Note - the oci run command will pull out the SystemdCgroups setting from config.
		err = RunNS(ctx, id.String(), b.Path(), "")
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		os.Exit(exitErr.ExitCode())
	}
	return err
}

// getCgroup will return a cgroup path and resources for the runtime to create.
func (l *Launcher) getCgroup() (path string, resources *specs.LinuxResources, err error) {
	if l.cfg.CGroupsJSON == "" {
		return "", nil, nil
	}
	path = cgroups.DefaultPathForPid(l.singularityConf.SystemdCgroups, -1)
	resources, err = cgroups.UnmarshalJSONResources(l.cfg.CGroupsJSON)
	if err != nil {
		return "", nil, err
	}
	return path, resources, nil
}

func mergeMap(a map[string]string, b map[string]string) map[string]string {
	for k, v := range b {
		a[k] = v
	}
	return a
}
