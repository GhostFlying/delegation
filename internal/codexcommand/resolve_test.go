package codexcommand

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestResolveAcceptsNativeExecutable(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(executable)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := filepath.EvalSymlinks(executable)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.NativePath != canonical || resolved.CommandPath == "" || len(resolved.UnsetEnvironment) != 4 {
		t.Fatalf("resolved native command = %#v", resolved)
	}
}

func TestResolveOfficialUnixNPMShim(t *testing.T) {
	if os.PathSeparator == '\\' {
		t.Skip("Unix symlink fixture")
	}
	root := t.TempDir()
	packageRoot := filepath.Join(root, "lib", "node_modules", "@openai", "codex")
	if err := os.MkdirAll(filepath.Join(packageRoot, "bin"), 0o700); err != nil {
		t.Fatal(err)
	}
	writeJSON(t, filepath.Join(packageRoot, "package.json"), map[string]any{
		"name": "@openai/codex", "version": "test", "bin": map[string]string{"codex": "bin/codex.js"},
	})
	shimTarget := filepath.Join(packageRoot, "bin", "codex.js")
	if err := os.WriteFile(shimTarget, []byte("#!/usr/bin/env node\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	commandPath := filepath.Join(root, "bin", "codex")
	if err := os.MkdirAll(filepath.Dir(commandPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(shimTarget, commandPath); err != nil {
		t.Fatal(err)
	}
	nativePath := installNativeFixture(t, packageRoot)
	canonicalPackageRoot, err := filepath.EvalSymlinks(packageRoot)
	if err != nil {
		t.Fatal(err)
	}

	resolved, err := Resolve(commandPath)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.CommandPath != commandPath || resolved.NativePath != nativePath ||
		resolved.Environment["CODEX_MANAGED_PACKAGE_ROOT"] != canonicalPackageRoot ||
		resolved.Environment["CODEX_MANAGED_BY_NPM"] != "1" {
		t.Fatalf("resolved npm shim = %#v", resolved)
	}
}

func TestResolveRejectsUnofficialScriptAndEscapingEntrypoint(t *testing.T) {
	script := filepath.Join(t.TempDir(), "codex.js")
	if err := os.WriteFile(script, []byte("#!/usr/bin/env node\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := Resolve(script); err == nil {
		t.Fatal("Resolve accepted an unofficial script")
	}

	root := t.TempDir()
	packageRoot := filepath.Join(root, "node_modules", "@openai", "codex")
	if err := os.MkdirAll(filepath.Join(packageRoot, "bin"), 0o700); err != nil {
		t.Fatal(err)
	}
	writeJSON(t, filepath.Join(packageRoot, "package.json"), map[string]any{
		"name": "@openai/codex", "version": "test", "bin": map[string]string{"codex": "bin/codex.js"},
	})
	shim := filepath.Join(packageRoot, "bin", "codex.js")
	if err := os.WriteFile(shim, []byte("#!/usr/bin/env node\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	spec, err := currentPlatformPackage()
	if err != nil {
		t.Fatal(err)
	}
	metadataRoot := filepath.Join(packageRoot, "node_modules", "@openai", spec.packageName, "vendor", spec.target)
	if err := os.MkdirAll(metadataRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	writeJSON(t, filepath.Join(metadataRoot, "codex-package.json"), packageMetadata{
		LayoutVersion: 1, Version: "test", Target: spec.target, Variant: "codex", Entrypoint: "../outside",
	})
	if _, err := Resolve(shim); err == nil {
		t.Fatal("Resolve accepted an escaping native entrypoint")
	}
}

func TestResolveFindsHoistedNativePackage(t *testing.T) {
	if os.PathSeparator == '\\' {
		t.Skip("Unix symlink fixture")
	}
	root := t.TempDir()
	packageRoot := filepath.Join(
		root,
		"node_modules", ".pnpm", "@openai+codex@test", "node_modules", "@openai", "codex",
	)
	if err := os.MkdirAll(filepath.Join(packageRoot, "bin"), 0o700); err != nil {
		t.Fatal(err)
	}
	writeJSON(t, filepath.Join(packageRoot, "package.json"), map[string]any{
		"name": "@openai/codex", "version": "test", "bin": map[string]string{"codex": "bin/codex.js"},
	})
	shimTarget := filepath.Join(packageRoot, "bin", "codex.js")
	if err := os.WriteFile(shimTarget, []byte("#!/usr/bin/env node\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	commandPath := filepath.Join(root, "bin", "codex")
	if err := os.MkdirAll(filepath.Dir(commandPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(shimTarget, commandPath); err != nil {
		t.Fatal(err)
	}
	nativePath := installNativeFixture(t, root)

	resolved, err := Resolve(commandPath)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.NativePath != nativePath || resolved.Environment["CODEX_MANAGED_BY_PNPM"] != "1" {
		t.Fatalf("resolved hoisted pnpm command = %#v", resolved)
	}
}

func TestResolvePackageBinaryFindsActualAncestorNodeModulesLayouts(t *testing.T) {
	root := t.TempDir()
	for _, test := range []struct {
		name              string
		packageRoot       string
		nativeInstallRoot string
	}{
		{
			name:              "npm",
			packageRoot:       filepath.Join(root, "npm", "node_modules", "@openai", "codex"),
			nativeInstallRoot: filepath.Join(root, "npm"),
		},
		{
			name: "pnpm",
			packageRoot: filepath.Join(
				root, "pnpm", "node_modules", ".pnpm", "@openai+codex@test",
				"node_modules", "@openai", "codex",
			),
			nativeInstallRoot: filepath.Join(root, "pnpm"),
		},
		{
			name: "bun",
			packageRoot: filepath.Join(
				root, ".bun", "install", "global", "node_modules", "@openai", "codex",
			),
			nativeInstallRoot: filepath.Join(root, ".bun", "install", "global"),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := os.MkdirAll(test.packageRoot, 0o700); err != nil {
				t.Fatal(err)
			}
			want := installNativeFixture(t, test.nativeInstallRoot)
			spec, err := currentPlatformPackage()
			if err != nil {
				t.Fatal(err)
			}

			got, err := resolvePackageBinary(test.packageRoot, "test", spec)
			if err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Fatalf("resolvePackageBinary() = %q, want %q", got, want)
			}
		})
	}
}

func TestResolvePackageBinaryIgnoresUnrelatedAncestorNodeModules(t *testing.T) {
	root := t.TempDir()
	packageRoot := filepath.Join(root, "install", "official-codex")
	if err := os.MkdirAll(packageRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	installNativeFixture(t, root)
	spec, err := currentPlatformPackage()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := resolvePackageBinary(packageRoot, "test", spec); err == nil {
		t.Fatal("resolvePackageBinary accepted a native package from an unrelated ancestor node_modules")
	}
}

func TestWindowsShimReferencePreservesUnicodeByteOffsets(t *testing.T) {
	root := filepath.Join(t.TempDir(), "\u0130")
	entrypoint := filepath.Join(root, "@openai", "codex", "bin", "codex.js")
	commandPath := filepath.Join(t.TempDir(), "codex.cmd")
	if err := os.WriteFile(commandPath, []byte(`"`+filepath.ToSlash(entrypoint)+`"`), 0o600); err != nil {
		t.Fatal(err)
	}
	roots, err := windowsShimReferencedPackageRoots(commandPath)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{filepath.Join(root, "@openai", "codex")}
	if !reflect.DeepEqual(roots, want) {
		t.Fatalf("Windows shim roots = %#v, want %#v", roots, want)
	}
}

func TestResolveRejectsMismatchedNativePackageMetadata(t *testing.T) {
	if os.PathSeparator == '\\' {
		t.Skip("Unix symlink fixture")
	}
	for name, mutate := range map[string]func(*packageMetadata){
		"version": func(metadata *packageMetadata) { metadata.Version = "other" },
		"target":  func(metadata *packageMetadata) { metadata.Target = "other-target" },
		"variant": func(metadata *packageMetadata) { metadata.Variant = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			packageRoot := filepath.Join(root, "node_modules", "@openai", "codex")
			if err := os.MkdirAll(filepath.Join(packageRoot, "bin"), 0o700); err != nil {
				t.Fatal(err)
			}
			writeJSON(t, filepath.Join(packageRoot, "package.json"), map[string]any{
				"name": "@openai/codex", "version": "test", "bin": map[string]string{"codex": "bin/codex.js"},
			})
			shim := filepath.Join(packageRoot, "bin", "codex.js")
			if err := os.WriteFile(shim, []byte("#!/usr/bin/env node\n"), 0o700); err != nil {
				t.Fatal(err)
			}
			installNativeFixture(t, packageRoot)
			spec, err := currentPlatformPackage()
			if err != nil {
				t.Fatal(err)
			}
			metadata := packageMetadata{
				LayoutVersion: 1, Version: "test", Target: spec.target,
				Variant: "codex", Entrypoint: "bin/" + spec.binaryName,
			}
			mutate(&metadata)
			writeJSON(t, filepath.Join(
				packageRoot, "node_modules", "@openai", spec.packageName, "vendor", spec.target,
				"codex-package.json",
			), metadata)
			if _, err := Resolve(shim); err == nil {
				t.Fatal("Resolve accepted mismatched native package metadata")
			}
		})
	}
}

func TestResolveRejectsNonExecutableNativePackage(t *testing.T) {
	if os.PathSeparator == '\\' {
		t.Skip("Unix execute bits")
	}
	root := t.TempDir()
	packageRoot := filepath.Join(root, "node_modules", "@openai", "codex")
	if err := os.MkdirAll(filepath.Join(packageRoot, "bin"), 0o700); err != nil {
		t.Fatal(err)
	}
	writeJSON(t, filepath.Join(packageRoot, "package.json"), map[string]any{
		"name": "@openai/codex", "version": "test", "bin": map[string]string{"codex": "bin/codex.js"},
	})
	shim := filepath.Join(packageRoot, "bin", "codex.js")
	if err := os.WriteFile(shim, []byte("#!/usr/bin/env node\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	nativePath := installNativeFixture(t, packageRoot)
	if err := os.Chmod(nativePath, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Resolve(shim); err == nil {
		t.Fatal("Resolve accepted a native package without an execute bit")
	}
}

func TestWindowsShimPackageRootCandidates(t *testing.T) {
	root := t.TempDir()
	tests := []struct {
		name        string
		commandPath string
		want        []string
	}{
		{
			name:        "global cmd",
			commandPath: filepath.Join(root, "npm", "codex.cmd"),
			want: []string{
				filepath.Join(root, "npm", "node_modules", "@openai", "codex"),
			},
		},
		{
			name:        "global bat",
			commandPath: filepath.Join(root, "npm", "CODEX.BAT"),
			want: []string{
				filepath.Join(root, "npm", "node_modules", "@openai", "codex"),
			},
		},
		{
			name:        "project local",
			commandPath: filepath.Join(root, "project", "node_modules", ".bin", "codex.cmd"),
			want: []string{
				filepath.Join(root, "project", "node_modules", ".bin", "node_modules", "@openai", "codex"),
				filepath.Join(root, "project", "node_modules", "@openai", "codex"),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := windowsShimPackageRootCandidates(test.commandPath)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("package roots = %q, want %q", got, test.want)
			}
		})
	}
}

func TestWindowsShimPackageRootCandidatesRejectUnofficialName(t *testing.T) {
	for _, commandPath := range []string{"other.cmd", "codex.ps1"} {
		if _, err := windowsShimPackageRootCandidates(commandPath); err == nil {
			t.Fatalf("accepted unofficial Windows shim %q", commandPath)
		}
	}
}

func TestOfficialPackageRootAcceptsWindowsNPMLayouts(t *testing.T) {
	root := t.TempDir()
	tests := []struct {
		name        string
		commandPath string
		packageRoot string
	}{
		{
			name:        "global",
			commandPath: filepath.Join(root, "npm", "codex.cmd"),
			packageRoot: filepath.Join(root, "npm", "node_modules", "@openai", "codex"),
		},
		{
			name:        "project local",
			commandPath: filepath.Join(root, "project", "node_modules", ".bin", "codex.bat"),
			packageRoot: filepath.Join(root, "project", "node_modules", "@openai", "codex"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := os.MkdirAll(test.packageRoot, 0o700); err != nil {
				t.Fatal(err)
			}
			writeJSON(t, filepath.Join(test.packageRoot, "package.json"), map[string]any{
				"name": "@openai/codex", "version": "test", "bin": map[string]string{"codex": "bin/codex.js"},
			})

			gotRoot, gotVersion, err := officialPackageRoot(test.commandPath, test.commandPath)
			if err != nil {
				t.Fatal(err)
			}
			canonicalRoot, err := filepath.EvalSymlinks(test.packageRoot)
			if err != nil {
				t.Fatal(err)
			}
			if gotRoot != canonicalRoot || gotVersion != "test" {
				t.Fatalf("official package = (%q, %q), want (%q, test)", gotRoot, gotVersion, canonicalRoot)
			}
		})
	}
}

func TestOfficialPackageRootDoesNotSkipInvalidWindowsCandidate(t *testing.T) {
	root := t.TempDir()
	commandPath := filepath.Join(root, "project", "node_modules", ".bin", "codex.cmd")
	firstCandidate := filepath.Join(filepath.Dir(commandPath), "node_modules", "@openai", "codex")
	localPackage := filepath.Join(root, "project", "node_modules", "@openai", "codex")
	for _, packageRoot := range []string{firstCandidate, localPackage} {
		if err := os.MkdirAll(packageRoot, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	writeJSON(t, filepath.Join(firstCandidate, "package.json"), map[string]any{
		"name": "unofficial", "version": "test", "bin": map[string]string{"codex": "bin/codex.js"},
	})
	writeJSON(t, filepath.Join(localPackage, "package.json"), map[string]any{
		"name": "@openai/codex", "version": "test", "bin": map[string]string{"codex": "bin/codex.js"},
	})

	if _, _, err := officialPackageRoot(commandPath, commandPath); err == nil {
		t.Fatal("accepted a later package root after an earlier candidate failed validation")
	}
}

func installNativeFixture(t *testing.T, packageRoot string) string {
	t.Helper()
	spec, err := currentPlatformPackage()
	if err != nil {
		t.Fatal(err)
	}
	metadataRoot := filepath.Join(packageRoot, "node_modules", "@openai", spec.packageName, "vendor", spec.target)
	nativePath := filepath.Join(metadataRoot, "bin", spec.binaryName)
	if err := os.MkdirAll(filepath.Dir(nativePath), 0o700); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(executable)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(nativePath, data, 0o700); err != nil {
		t.Fatal(err)
	}
	writeJSON(t, filepath.Join(metadataRoot, "codex-package.json"), packageMetadata{
		LayoutVersion: 1, Version: "test", Target: spec.target, Variant: "codex", Entrypoint: "bin/" + spec.binaryName,
	})
	canonical, err := filepath.EvalSymlinks(nativePath)
	if err != nil {
		t.Fatal(err)
	}
	return canonical
}

func writeJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}
