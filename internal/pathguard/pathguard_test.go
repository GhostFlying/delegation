package pathguard

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateBrokerAuthorityAcceptsDistinctPaths(t *testing.T) {
	root := t.TempDir()
	err := ValidateBrokerAuthority(
		filepath.Join(root, "config.json"),
		filepath.Join(root, "state", "broker.sqlite3"),
		filepath.Join(root, "secrets", "broker.token"),
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestValidateTailscaleAuthorityAcceptsDefaultLayouts(t *testing.T) {
	t.Run("broker", func(t *testing.T) {
		root := t.TempDir()
		err := ValidateBrokerTailscaleAuthority(
			filepath.Join(root, "broker.json"),
			filepath.Join(root, "state", "broker.sqlite3"),
			filepath.Join(root, "secrets", "broker.token"),
			filepath.Join(root, "state", "tailscale", "broker"),
			filepath.Join(root, "secrets", "tailscale-auth.key"),
		)
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("peer", func(t *testing.T) {
		root := t.TempDir()
		err := ValidatePeerTailscaleAuthority(
			filepath.Join(root, "peer.json"),
			filepath.Join(root, "state", "peer.sqlite3"),
			filepath.Join(root, "secrets", "peer.token"),
			filepath.Join(root, "cli"),
			filepath.Join(root, "workspaces"),
			filepath.Join(root, "state", "tailscale", "peer"),
			filepath.Join(root, "secrets", "tailscale-auth.key"),
		)
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestValidateTailscaleAuthorityAcceptsSiblingNamespaces(t *testing.T) {
	for _, test := range []struct {
		name          string
		authorityRoot string
		tailscaleRoot string
	}{
		{name: "instances", authorityRoot: "instances/alpha", tailscaleRoot: "instances/beta"},
		{name: "host kinds", authorityRoot: "hosts/local", tailscaleRoot: "hosts/remote"},
		{name: "roles", authorityRoot: "roles/broker", tailscaleRoot: "roles/peer"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			authorityRoot := filepath.Join(root, filepath.FromSlash(test.authorityRoot))
			tailscaleRoot := filepath.Join(root, filepath.FromSlash(test.tailscaleRoot))
			err := ValidateBrokerTailscaleAuthority(
				filepath.Join(authorityRoot, "broker.json"),
				filepath.Join(authorityRoot, "state", "broker.sqlite3"),
				filepath.Join(authorityRoot, "secrets", "broker.token"),
				filepath.Join(tailscaleRoot, "state"),
				filepath.Join(tailscaleRoot, "secrets", "auth.key"),
			)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestValidateTailscaleAuthorityRejectsEveryAuthorityAlias(t *testing.T) {
	t.Run("broker", func(t *testing.T) {
		root := t.TempDir()
		configPath := filepath.Join(root, "broker.json")
		statePath := filepath.Join(root, "state", "broker.sqlite3")
		tokenPath := filepath.Join(root, "secrets", "broker.token")
		tailscaleStateDir := filepath.Join(root, "state", "tailscale")
		authKeyPath := filepath.Join(root, "secrets", "tailscale-auth.key")
		for _, authority := range brokerAuthorityFiles(configPath, statePath, tokenPath) {
			t.Run(authority.name+" enrollment key", func(t *testing.T) {
				err := ValidateBrokerTailscaleAuthority(
					configPath,
					statePath,
					tokenPath,
					tailscaleStateDir,
					authority.path,
				)
				want := "Tailscale enrollment key path conflicts with " + authority.name
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("ValidateBrokerTailscaleAuthority() error = %v, want %q", err, want)
				}
			})
			t.Run(authority.name+" state directory", func(t *testing.T) {
				err := ValidateBrokerTailscaleAuthority(
					configPath,
					statePath,
					tokenPath,
					authority.path,
					authKeyPath,
				)
				want := "Tailscale state directory must not be inside " + authority.name
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("ValidateBrokerTailscaleAuthority() error = %v, want %q", err, want)
				}
			})
		}
	})

	t.Run("peer", func(t *testing.T) {
		root := t.TempDir()
		configPath := filepath.Join(root, "peer.json")
		statePath := filepath.Join(root, "state", "peer.sqlite3")
		tokenPath := filepath.Join(root, "secrets", "peer.token")
		codexHome := filepath.Join(root, "cli")
		workspaceRoot := filepath.Join(root, "workspaces")
		tailscaleStateDir := filepath.Join(root, "state", "tailscale")
		authKeyPath := filepath.Join(root, "secrets", "tailscale-auth.key")
		for _, authority := range peerAuthorityFiles(configPath, statePath, tokenPath) {
			t.Run(authority.name+" enrollment key", func(t *testing.T) {
				err := ValidatePeerTailscaleAuthority(
					configPath,
					statePath,
					tokenPath,
					codexHome,
					workspaceRoot,
					tailscaleStateDir,
					authority.path,
				)
				want := "Tailscale enrollment key path conflicts with " + authority.name
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("ValidatePeerTailscaleAuthority() error = %v, want %q", err, want)
				}
			})
			t.Run(authority.name+" state directory", func(t *testing.T) {
				err := ValidatePeerTailscaleAuthority(
					configPath,
					statePath,
					tokenPath,
					codexHome,
					workspaceRoot,
					authority.path,
					authKeyPath,
				)
				want := "Tailscale state directory must not be inside " + authority.name
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("ValidatePeerTailscaleAuthority() error = %v, want %q", err, want)
				}
			})
		}
	})
}

func TestValidateTailscaleAuthorityRejectsDerivedLeaseAliases(t *testing.T) {
	for _, role := range []string{"broker", "peer"} {
		t.Run(role, func(t *testing.T) {
			root := t.TempDir()
			defaultConfigPath := filepath.Join(root, role+".json")
			defaultStatePath := filepath.Join(root, "state", role+".sqlite3")
			defaultTokenPath := filepath.Join(root, "secrets", role+".token")
			defaultAuthKeyPath := filepath.Join(root, "secrets", "tailscale-auth.key")
			tailscaleStateDir := filepath.Join(root, "tailscale")
			leasePath := tailscaleStateDir + ".tailscale.lock"
			tokenName := role + " token"
			if role == "broker" {
				tokenName = "broker master token"
			}
			for _, test := range []struct {
				name       string
				configPath string
				statePath  string
				tokenPath  string
				authKey    string
				want       string
			}{
				{
					name:       "configuration",
					configPath: leasePath,
					statePath:  defaultStatePath,
					tokenPath:  defaultTokenPath,
					authKey:    defaultAuthKeyPath,
					want:       role + " configuration",
				},
				{
					name:       "token",
					configPath: defaultConfigPath,
					statePath:  defaultStatePath,
					tokenPath:  leasePath,
					authKey:    defaultAuthKeyPath,
					want:       tokenName,
				},
				{
					name:       "state",
					configPath: defaultConfigPath,
					statePath:  leasePath,
					tokenPath:  defaultTokenPath,
					authKey:    defaultAuthKeyPath,
					want:       role + " state",
				},
				{
					name:       "enrollment key",
					configPath: defaultConfigPath,
					statePath:  defaultStatePath,
					tokenPath:  defaultTokenPath,
					authKey:    leasePath,
					want:       "Tailscale enrollment key",
				},
			} {
				t.Run(test.name, func(t *testing.T) {
					var err error
					if role == "broker" {
						err = ValidateBrokerTailscaleAuthority(
							test.configPath,
							test.statePath,
							test.tokenPath,
							tailscaleStateDir,
							test.authKey,
						)
					} else {
						err = ValidatePeerTailscaleAuthority(
							test.configPath,
							test.statePath,
							test.tokenPath,
							filepath.Join(root, "cli"),
							filepath.Join(root, "workspaces"),
							tailscaleStateDir,
							test.authKey,
						)
					}
					want := "Tailscale state directory lease path conflicts with " + test.want
					if err == nil || !strings.Contains(err.Error(), want) {
						t.Fatalf("Tailscale authority validation error = %v, want %q", err, want)
					}
				})
			}
		})
	}

	t.Run("existing broker WAL alias", func(t *testing.T) {
		root := t.TempDir()
		statePath := filepath.Join(root, "broker.sqlite3")
		walPath := statePath + "-wal"
		tailscaleStateDir := filepath.Join(root, "tailscale")
		leasePath := tailscaleStateDir + ".tailscale.lock"
		if err := os.WriteFile(walPath, []byte("wal"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(walPath, leasePath); err != nil {
			t.Skipf("creating a hard link is unavailable: %v", err)
		}
		err := ValidateBrokerTailscaleAuthority(
			filepath.Join(root, "broker.json"),
			statePath,
			filepath.Join(root, "broker.token"),
			tailscaleStateDir,
			filepath.Join(root, "tailscale-auth.key"),
		)
		want := "Tailscale state directory lease path conflicts with broker WAL"
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("ValidateBrokerTailscaleAuthority() error = %v, want %q", err, want)
		}
	})

	t.Run("derived lease contains broker configuration", func(t *testing.T) {
		root := t.TempDir()
		tailscaleStateDir := filepath.Join(root, "tailscale")
		leasePath := tailscaleStateDir + ".tailscale.lock"
		err := ValidateBrokerTailscaleAuthority(
			filepath.Join(leasePath, "broker.json"),
			filepath.Join(root, "broker.sqlite3"),
			filepath.Join(root, "broker.token"),
			tailscaleStateDir,
			filepath.Join(root, "tailscale-auth.key"),
		)
		want := "broker configuration must not be inside Tailscale state directory lease"
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("ValidateBrokerTailscaleAuthority() error = %v, want %q", err, want)
		}
	})
}

func TestValidateTailscaleAuthorityRejectsCaseFoldedEnrollmentKeyAlias(t *testing.T) {
	root := t.TempDir()
	err := ValidateBrokerTailscaleAuthority(
		filepath.Join(root, "Broker.JSON"),
		filepath.Join(root, "state", "broker.sqlite3"),
		filepath.Join(root, "secrets", "broker.token"),
		filepath.Join(root, "state", "tailscale"),
		filepath.Join(root, "broker.json"),
	)
	if err == nil || !strings.Contains(err.Error(), "Tailscale enrollment key path conflicts with broker configuration") {
		t.Fatalf("ValidateBrokerTailscaleAuthority() error = %v", err)
	}
}

func TestValidateTailscaleAuthorityRejectsExistingFileAliases(t *testing.T) {
	t.Run("enrollment key aliases authority", func(t *testing.T) {
		root := t.TempDir()
		configPath := filepath.Join(root, "broker.json")
		authKeyPath := filepath.Join(root, "tailscale-auth.key")
		if err := os.WriteFile(configPath, []byte("config"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(configPath, authKeyPath); err != nil {
			t.Skipf("creating a hard link is unavailable: %v", err)
		}
		err := ValidateBrokerTailscaleAuthority(
			configPath,
			filepath.Join(root, "state", "broker.sqlite3"),
			filepath.Join(root, "secrets", "broker.token"),
			filepath.Join(root, "state", "tailscale"),
			authKeyPath,
		)
		if err == nil || !strings.Contains(err.Error(), "Tailscale enrollment key path conflicts with broker configuration") {
			t.Fatalf("ValidateBrokerTailscaleAuthority() error = %v", err)
		}
	})

	t.Run("enrollment key has another hard link", func(t *testing.T) {
		root := t.TempDir()
		authKeyPath := filepath.Join(root, "tailscale-auth.key")
		if err := os.WriteFile(authKeyPath, []byte("enrollment key"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(authKeyPath, filepath.Join(root, "second-link")); err != nil {
			t.Skipf("creating a hard link is unavailable: %v", err)
		}
		err := ValidateBrokerTailscaleAuthority(
			filepath.Join(root, "broker.json"),
			filepath.Join(root, "state", "broker.sqlite3"),
			filepath.Join(root, "secrets", "broker.token"),
			filepath.Join(root, "state", "tailscale"),
			authKeyPath,
		)
		if err == nil || !strings.Contains(err.Error(), "Tailscale enrollment key has unexpected hard-link count 2") {
			t.Fatalf("ValidateBrokerTailscaleAuthority() error = %v", err)
		}
	})

	t.Run("state directory has same file identity as authority", func(t *testing.T) {
		root := t.TempDir()
		configPath := filepath.Join(root, "broker.json")
		tailscaleStateDir := filepath.Join(root, "tailscale-state")
		if err := os.WriteFile(configPath, []byte("config"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(configPath, tailscaleStateDir); err != nil {
			t.Skipf("creating a hard link is unavailable: %v", err)
		}
		err := ValidateBrokerTailscaleAuthority(
			configPath,
			filepath.Join(root, "state", "broker.sqlite3"),
			filepath.Join(root, "secrets", "broker.token"),
			tailscaleStateDir,
			filepath.Join(root, "secrets", "tailscale-auth.key"),
		)
		if err == nil || !strings.Contains(err.Error(), "Tailscale state directory path conflicts with broker configuration") {
			t.Fatalf("ValidateBrokerTailscaleAuthority() error = %v", err)
		}
	})
}

func TestValidateTailscaleAuthorityRejectsSymlinkedFutureAliases(t *testing.T) {
	targetRoot := t.TempDir()
	aliasRoot := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(targetRoot, aliasRoot); err != nil {
		t.Skipf("creating a directory symlink is unavailable: %v", err)
	}

	t.Run("enrollment key aliases future configuration", func(t *testing.T) {
		configPath := filepath.Join(targetRoot, "future", "broker.json")
		err := ValidateBrokerTailscaleAuthority(
			configPath,
			filepath.Join(targetRoot, "state", "broker.sqlite3"),
			filepath.Join(targetRoot, "secrets", "broker.token"),
			filepath.Join(targetRoot, "state", "tailscale"),
			filepath.Join(aliasRoot, "future", "broker.json"),
		)
		if err == nil || !strings.Contains(err.Error(), "Tailscale enrollment key path conflicts with broker configuration") {
			t.Fatalf("ValidateBrokerTailscaleAuthority() error = %v", err)
		}
	})

	t.Run("future authority is inside state alias", func(t *testing.T) {
		err := ValidateBrokerTailscaleAuthority(
			filepath.Join(targetRoot, "future-state", "broker.json"),
			filepath.Join(targetRoot, "state", "broker.sqlite3"),
			filepath.Join(targetRoot, "secrets", "broker.token"),
			filepath.Join(aliasRoot, "future-state"),
			filepath.Join(targetRoot, "secrets", "tailscale-auth.key"),
		)
		if err == nil || !strings.Contains(err.Error(), "broker configuration must not be inside Tailscale state directory") {
			t.Fatalf("ValidateBrokerTailscaleAuthority() error = %v", err)
		}
	})
}

func TestValidateTailscaleAuthorityRejectsMutualContainment(t *testing.T) {
	for _, role := range []string{"broker", "peer"} {
		t.Run(role, func(t *testing.T) {
			root := t.TempDir()
			configPath := filepath.Join(root, role+".json")
			statePath := filepath.Join(root, "state", role+".sqlite3")
			tokenPath := filepath.Join(root, "secrets", role+".token")
			codexHome := filepath.Join(root, "cli")
			workspaceRoot := filepath.Join(root, "workspaces")
			authKeyPath := filepath.Join(root, "secrets", "tailscale-auth.key")
			validate := func(
				configPath, tokenPath, tailscaleStateDir, tailscaleAuthKeyPath string,
			) error {
				if role == "broker" {
					return ValidateBrokerTailscaleAuthority(
						configPath,
						statePath,
						tokenPath,
						tailscaleStateDir,
						tailscaleAuthKeyPath,
					)
				}
				return ValidatePeerTailscaleAuthority(
					configPath,
					statePath,
					tokenPath,
					codexHome,
					workspaceRoot,
					tailscaleStateDir,
					tailscaleAuthKeyPath,
				)
			}
			configurationName := role + " configuration"
			tokenName := role + " "
			if role == "broker" {
				tokenName += "master token"
			} else {
				tokenName += "token"
			}
			for _, test := range []struct {
				name              string
				configPath        string
				tokenPath         string
				tailscaleStateDir string
				authKeyPath       string
				want              string
			}{
				{
					name:              "state contains authority",
					configPath:        filepath.Join(root, "tailscale", role+".json"),
					tokenPath:         tokenPath,
					tailscaleStateDir: filepath.Join(root, "tailscale"),
					authKeyPath:       authKeyPath,
					want:              configurationName + " must not be inside Tailscale state directory",
				},
				{
					name:              "authority path contains state",
					configPath:        configPath,
					tokenPath:         filepath.Join(root, "authority"),
					tailscaleStateDir: filepath.Join(root, "authority", "tailscale"),
					authKeyPath:       authKeyPath,
					want:              "Tailscale state directory must not be inside " + tokenName,
				},
				{
					name:              "state contains enrollment key",
					configPath:        configPath,
					tokenPath:         tokenPath,
					tailscaleStateDir: filepath.Join(root, "tailscale"),
					authKeyPath:       filepath.Join(root, "tailscale", "auth.key"),
					want:              "Tailscale enrollment key must not be inside Tailscale state directory",
				},
				{
					name:              "enrollment key path contains state",
					configPath:        configPath,
					tokenPath:         tokenPath,
					tailscaleStateDir: filepath.Join(root, "key-path", "tailscale"),
					authKeyPath:       filepath.Join(root, "key-path"),
					want:              "Tailscale state directory must not be inside Tailscale enrollment key",
				},
			} {
				t.Run(test.name, func(t *testing.T) {
					err := validate(
						test.configPath,
						test.tokenPath,
						test.tailscaleStateDir,
						test.authKeyPath,
					)
					if err == nil || !strings.Contains(err.Error(), test.want) {
						t.Fatalf("Tailscale authority validation error = %v, want %q", err, test.want)
					}
				})
			}
		})
	}
}

func TestValidatePeerTailscaleAuthorityRejectsManagedDirectoryRelationships(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "peer.json")
	statePath := filepath.Join(root, "state", "peer.sqlite3")
	tokenPath := filepath.Join(root, "secrets", "peer.token")
	defaultCodexHome := filepath.Join(root, "cli")
	defaultWorkspaceRoot := filepath.Join(root, "workspaces")
	defaultTailscaleStateDir := filepath.Join(root, "tailscale")
	defaultAuthKeyPath := filepath.Join(root, "secrets", "tailscale-auth.key")
	for _, test := range []struct {
		name              string
		codexHome         string
		workspaceRoot     string
		tailscaleStateDir string
		authKeyPath       string
		want              string
	}{
		{
			name:              "state inside CODEX_HOME",
			codexHome:         defaultCodexHome,
			workspaceRoot:     defaultWorkspaceRoot,
			tailscaleStateDir: filepath.Join(defaultCodexHome, "tailscale"),
			authKeyPath:       defaultAuthKeyPath,
			want:              "Tailscale state directory must not be inside worker CODEX_HOME",
		},
		{
			name:              "CODEX_HOME inside state",
			codexHome:         filepath.Join(defaultTailscaleStateDir, "cli"),
			workspaceRoot:     defaultWorkspaceRoot,
			tailscaleStateDir: defaultTailscaleStateDir,
			authKeyPath:       defaultAuthKeyPath,
			want:              "worker CODEX_HOME must not be inside Tailscale state directory",
		},
		{
			name:              "state inside workspace",
			codexHome:         defaultCodexHome,
			workspaceRoot:     defaultWorkspaceRoot,
			tailscaleStateDir: filepath.Join(defaultWorkspaceRoot, "tailscale"),
			authKeyPath:       defaultAuthKeyPath,
			want:              "Tailscale state directory must not be inside worker workspace root",
		},
		{
			name:              "workspace inside state",
			codexHome:         defaultCodexHome,
			workspaceRoot:     filepath.Join(defaultTailscaleStateDir, "workspaces"),
			tailscaleStateDir: defaultTailscaleStateDir,
			authKeyPath:       defaultAuthKeyPath,
			want:              "worker workspace root must not be inside Tailscale state directory",
		},
		{
			name:              "enrollment key inside CODEX_HOME",
			codexHome:         defaultCodexHome,
			workspaceRoot:     defaultWorkspaceRoot,
			tailscaleStateDir: defaultTailscaleStateDir,
			authKeyPath:       filepath.Join(defaultCodexHome, "auth.key"),
			want:              "Tailscale enrollment key must not be inside worker CODEX_HOME",
		},
		{
			name:              "enrollment key inside workspace",
			codexHome:         defaultCodexHome,
			workspaceRoot:     defaultWorkspaceRoot,
			tailscaleStateDir: defaultTailscaleStateDir,
			authKeyPath:       filepath.Join(defaultWorkspaceRoot, "auth.key"),
			want:              "Tailscale enrollment key must not be inside worker workspace root",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := ValidatePeerTailscaleAuthority(
				configPath,
				statePath,
				tokenPath,
				test.codexHome,
				test.workspaceRoot,
				test.tailscaleStateDir,
				test.authKeyPath,
			)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("ValidatePeerTailscaleAuthority() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestValidatePeerTailscaleAuthorityPreservesRuntimeAuthority(t *testing.T) {
	root := t.TempDir()
	codexHome := filepath.Join(root, "cli")
	workspaceRoot := filepath.Join(root, "workspaces")
	tailscaleStateDir := filepath.Join(root, "tailscale")
	authKeyPath := filepath.Join(root, "secrets", "tailscale-auth.key")

	err := ValidatePeerTailscaleAuthority(
		filepath.Join(codexHome, "peer.json"),
		filepath.Join(root, "state", "peer.sqlite3"),
		filepath.Join(root, "secrets", "peer.token"),
		codexHome,
		workspaceRoot,
		tailscaleStateDir,
		authKeyPath,
	)
	if err == nil || !strings.Contains(err.Error(), "peer configuration must not be inside worker CODEX_HOME") {
		t.Fatalf("ValidatePeerTailscaleAuthority() error = %v", err)
	}

	err = ValidatePeerTailscaleAuthority(
		filepath.Join(root, "peer.json"),
		filepath.Join(root, "state", "peer.sqlite3"),
		filepath.Join(root, "secrets", "peer.token"),
		codexHome,
		filepath.Join(codexHome, "workspaces"),
		tailscaleStateDir,
		authKeyPath,
	)
	if err == nil || !strings.Contains(err.Error(), "must not contain one another") {
		t.Fatalf("ValidatePeerTailscaleAuthority() error = %v", err)
	}
}

func TestValidateTailscaleAuthorityRejectsRelativeAndParentPaths(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "broker.json")
	statePath := filepath.Join(root, "state", "broker.sqlite3")
	tokenPath := filepath.Join(root, "secrets", "broker.token")
	tailscaleStateDir := filepath.Join(root, "tailscale")
	authKeyPath := filepath.Join(root, "secrets", "tailscale-auth.key")
	for _, test := range []struct {
		name              string
		tailscaleStateDir string
		authKeyPath       string
		want              string
	}{
		{
			name:              "relative state",
			tailscaleStateDir: "tailscale",
			authKeyPath:       authKeyPath,
			want:              "guarded path must be absolute",
		},
		{
			name:              "relative enrollment key",
			tailscaleStateDir: tailscaleStateDir,
			authKeyPath:       "auth.key",
			want:              "guarded path must be absolute",
		},
		{
			name: "parent component in state",
			tailscaleStateDir: filepath.Join(root, "state") + string(filepath.Separator) +
				".." + string(filepath.Separator) + "tailscale",
			authKeyPath: authKeyPath,
			want:        "parent path components",
		},
		{
			name:              "parent component in enrollment key",
			tailscaleStateDir: tailscaleStateDir,
			authKeyPath: filepath.Join(root, "secrets") + string(filepath.Separator) +
				".." + string(filepath.Separator) + "auth.key",
			want: "parent path components",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateBrokerTailscaleAuthority(
				configPath,
				statePath,
				tokenPath,
				test.tailscaleStateDir,
				test.authKeyPath,
			)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("ValidateBrokerTailscaleAuthority() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestValidateTailscaleAuthorityDoesNotCreateFuturePaths(t *testing.T) {
	root := t.TempDir()
	futureRoot := filepath.Join(root, "future")
	err := ValidateBrokerTailscaleAuthority(
		filepath.Join(futureRoot, "config", "broker.json"),
		filepath.Join(futureRoot, "state", "broker.sqlite3"),
		filepath.Join(futureRoot, "secrets", "broker.token"),
		filepath.Join(futureRoot, "state", "tailscale"),
		filepath.Join(futureRoot, "secrets", "tailscale-auth.key"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(futureRoot); !os.IsNotExist(err) {
		t.Fatalf("future authority root was created: %v", err)
	}
}

func TestCanonicalFuturePathRejectsSymbolicLinkLoopAndDepth(t *testing.T) {
	root := t.TempDir()
	loopPath := filepath.Join(root, "loop")
	if err := os.Symlink(filepath.Base(loopPath), loopPath); err != nil {
		t.Skipf("creating a symbolic-link loop is unavailable: %v", err)
	}
	if _, err := canonicalFuturePath(loopPath); err == nil ||
		!strings.Contains(err.Error(), "too many symbolic links") {
		t.Fatalf("canonicalFuturePath() error = %v", err)
	}
	if _, err := resolveFuturePath(loopPath, 255); err == nil ||
		!strings.Contains(err.Error(), "too many symbolic links") {
		t.Fatalf("resolveFuturePath() error = %v", err)
	}
}

func TestValidatePeerAuthorityRejectsAliases(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.json")
	tokenPath := filepath.Join(root, "device.token")
	statePath := filepath.Join(root, "peer.sqlite3")
	if err := ValidatePeerAuthority(configPath, statePath, tokenPath); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, []byte("config"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(configPath, tokenPath); err != nil {
		t.Skipf("creating a hard link is unavailable: %v", err)
	}
	if err := ValidatePeerAuthority(configPath, statePath, tokenPath); err == nil ||
		!strings.Contains(err.Error(), "peer token") {
		t.Fatalf("ValidatePeerAuthority() error = %v", err)
	}
}

func TestValidatePeerAuthorityRejectsStateSidecarAliases(t *testing.T) {
	root := t.TempDir()
	statePath := filepath.Join(root, "peer.sqlite3")
	if err := os.WriteFile(statePath, []byte("state"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(statePath, statePath+"-wal"); err != nil {
		t.Skipf("creating a hard link is unavailable: %v", err)
	}
	err := ValidatePeerAuthority(filepath.Join(root, "peer.json"), statePath, "")
	if err == nil || !strings.Contains(err.Error(), "peer WAL path conflicts with peer state") {
		t.Fatalf("ValidatePeerAuthority() error = %v", err)
	}
}

func TestValidatePeerRuntimeAuthorityRejectsFileDirectoryAliases(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "peer.json")
	statePath := filepath.Join(root, "peer.sqlite3")
	tokenPath := filepath.Join(root, "peer.token")
	codexHome := filepath.Join(root, "codex")
	workspaceRoot := filepath.Join(root, "workspaces")
	if err := ValidatePeerRuntimeAuthority(
		configPath,
		statePath,
		tokenPath,
		codexHome,
		workspaceRoot,
	); err != nil {
		t.Fatal(err)
	}
	for name, paths := range map[string][2]string{
		"state and CODEX_HOME":    {statePath, statePath},
		"token and workspace":     {codexHome, tokenPath},
		"managed directories":     {codexHome, codexHome},
		"state WAL and workspace": {codexHome, statePath + "-wal"},
		"workspace inside home":   {codexHome, filepath.Join(codexHome, "workspaces")},
		"home inside workspace":   {filepath.Join(workspaceRoot, "codex"), workspaceRoot},
	} {
		t.Run(name, func(t *testing.T) {
			if err := ValidatePeerRuntimeAuthority(
				configPath,
				statePath,
				tokenPath,
				paths[0],
				paths[1],
			); err == nil {
				t.Fatal("ValidatePeerRuntimeAuthority accepted an alias")
			}
		})
	}
}

func TestValidatePeerRuntimeAuthorityRejectsAuthorityInsideManagedDirectories(t *testing.T) {
	root := t.TempDir()
	codexHome := filepath.Join(root, "codex")
	workspaceRoot := filepath.Join(root, "workspaces")
	for name, paths := range map[string]struct {
		config string
		state  string
		token  string
	}{
		"configuration inside CODEX_HOME": {
			config: filepath.Join(codexHome, "peer.json"),
			state:  filepath.Join(root, "state", "peer.sqlite3"),
		},
		"state inside workspace": {
			config: filepath.Join(root, "peer.json"),
			state:  filepath.Join(workspaceRoot, "state", "peer.sqlite3"),
		},
		"token inside CODEX_HOME": {
			config: filepath.Join(root, "peer.json"),
			state:  filepath.Join(root, "state", "peer.sqlite3"),
			token:  filepath.Join(codexHome, "peer.token"),
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := ValidatePeerRuntimeAuthority(
				paths.config,
				paths.state,
				paths.token,
				codexHome,
				workspaceRoot,
			)
			if err == nil {
				t.Fatal("ValidatePeerRuntimeAuthority accepted managed authority")
			}
		})
	}
}

func TestValidateManagedExecutableRejectsManagedDirectoriesAndAliases(t *testing.T) {
	root := t.TempDir()
	codexHome := filepath.Join(root, "codex")
	workspaceRoot := filepath.Join(root, "workspaces")
	executable := filepath.Join(root, "bin", "git")
	if err := ValidateManagedExecutable("Git binary", executable, codexHome, workspaceRoot); err != nil {
		t.Fatal(err)
	}
	for name, candidate := range map[string]string{
		"inside CODEX_HOME": filepath.Join(codexHome, "bin", "git"),
		"inside workspace":  filepath.Join(workspaceRoot, "tools", "git"),
	} {
		t.Run(name, func(t *testing.T) {
			if err := ValidateManagedExecutable(
				"Git binary", candidate, codexHome, workspaceRoot,
			); err == nil {
				t.Fatal("ValidateManagedExecutable accepted a managed executable")
			}
		})
	}
	if err := os.MkdirAll(filepath.Join(workspaceRoot, "tools"), 0o700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "git-link")
	if err := os.Symlink(filepath.Join(workspaceRoot, "tools", "git"), alias); err != nil {
		t.Skipf("creating a symbolic link is unavailable: %v", err)
	}
	if err := ValidateManagedExecutable("Git binary", alias, codexHome, workspaceRoot); err == nil {
		t.Fatal("ValidateManagedExecutable accepted a symlink into the workspace")
	}
}

func TestValidatePeerRuntimeAuthorityRejectsParentComponentsBeforeResolvingLinks(t *testing.T) {
	root := t.TempDir()
	workspaceRoot := filepath.Join(root, "workspaces")
	child := filepath.Join(workspaceRoot, "child")
	if err := os.MkdirAll(child, 0o700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(child, alias); err != nil {
		t.Skipf("creating a directory symlink is unavailable: %v", err)
	}
	err := ValidatePeerRuntimeAuthority(
		filepath.Join(root, "peer.json"),
		alias+string(filepath.Separator)+".."+string(filepath.Separator)+"peer.sqlite3",
		filepath.Join(root, "peer.token"),
		filepath.Join(root, "codex"),
		workspaceRoot,
	)
	if err == nil || !strings.Contains(err.Error(), "parent path components") {
		t.Fatalf("ValidatePeerRuntimeAuthority() error = %v", err)
	}
}

func TestValidatePeerServiceEnvironmentRejectsAuthorityAndManagedDirectoryAliases(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "peer.json")
	statePath := filepath.Join(root, "state", "peer.sqlite3")
	tokenPath := filepath.Join(root, "secrets", "peer.token")
	codexHome := filepath.Join(root, "codex")
	workspaceRoot := filepath.Join(root, "workspaces")
	environmentPath := filepath.Join(root, "secrets", "peer.env")
	if err := ValidatePeerServiceEnvironment(
		environmentPath,
		configPath,
		statePath,
		tokenPath,
		codexHome,
		workspaceRoot,
	); err != nil {
		t.Fatal(err)
	}
	for name, candidate := range map[string]string{
		"configuration":    configPath,
		"state sidecar":    statePath + "-wal",
		"CODEX_HOME":       filepath.Join(codexHome, "peer.env"),
		"workspace":        filepath.Join(workspaceRoot, "nested", "peer.env"),
		"case folded file": filepath.Join(root, "SECRETS", "PEER.TOKEN"),
	} {
		t.Run(name, func(t *testing.T) {
			if err := ValidatePeerServiceEnvironment(
				candidate,
				configPath,
				statePath,
				tokenPath,
				codexHome,
				workspaceRoot,
			); err == nil {
				t.Fatal("ValidatePeerServiceEnvironment accepted a conflicting path")
			}
		})
	}
}

func TestValidatePeerTailscaleServiceEnvironmentRejectsTailscaleAuthority(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "peer.json")
	statePath := filepath.Join(root, "state", "peer.sqlite3")
	tokenPath := filepath.Join(root, "secrets", "peer.token")
	codexHome := filepath.Join(root, "codex")
	workspaceRoot := filepath.Join(root, "workspaces")
	tailscaleStateDir := filepath.Join(root, "tailscale")
	authKeyPath := filepath.Join(root, "secrets", "tailscale-auth.key")
	validEnvironment := filepath.Join(root, "secrets", "peer.env")
	if err := ValidatePeerTailscaleServiceEnvironment(
		validEnvironment,
		configPath,
		statePath,
		tokenPath,
		codexHome,
		workspaceRoot,
		tailscaleStateDir,
		authKeyPath,
	); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name        string
		environment string
		want        string
	}{
		{
			name:        "derived lease",
			environment: tailscaleStateDir + ".tailscale.lock",
			want:        "peer service environment path conflicts with Tailscale state directory lease",
		},
		{
			name:        "enrollment key",
			environment: authKeyPath,
			want:        "peer service environment path conflicts with Tailscale enrollment key",
		},
		{
			name:        "inside state",
			environment: filepath.Join(tailscaleStateDir, "peer.env"),
			want:        "peer service environment must not be inside Tailscale state directory",
		},
		{
			name:        "inside derived lease",
			environment: filepath.Join(tailscaleStateDir+".tailscale.lock", "peer.env"),
			want:        "peer service environment must not be inside Tailscale state directory lease",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := ValidatePeerTailscaleServiceEnvironment(
				test.environment,
				configPath,
				statePath,
				tokenPath,
				codexHome,
				workspaceRoot,
				tailscaleStateDir,
				authKeyPath,
			)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf(
					"ValidatePeerTailscaleServiceEnvironment() error = %v, want %q",
					err,
					test.want,
				)
			}
		})
	}
}

func TestValidatePeerServiceEnvironmentRejectsHardLinkedAuthority(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "peer.json")
	environmentPath := filepath.Join(root, "peer.env")
	if err := os.WriteFile(configPath, []byte("authority"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(configPath, environmentPath); err != nil {
		t.Skipf("creating a hard link is unavailable: %v", err)
	}
	err := ValidatePeerServiceEnvironment(
		environmentPath,
		configPath,
		filepath.Join(root, "peer.sqlite3"),
		filepath.Join(root, "peer.token"),
		filepath.Join(root, "codex"),
		filepath.Join(root, "workspaces"),
	)
	if err == nil || !strings.Contains(err.Error(), "peer configuration") {
		t.Fatalf("ValidatePeerServiceEnvironment() error = %v", err)
	}
}

func TestValidatePeerRuntimeAuthorityRejectsHardLinkHiddenInWorkspace(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "secrets", "peer.json")
	workspaceRoot := filepath.Join(root, "workspaces")
	if err := os.MkdirAll(filepath.Dir(configPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(workspaceRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, []byte("authority"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(configPath, filepath.Join(workspaceRoot, "peer.json")); err != nil {
		t.Skipf("creating a hard link is unavailable: %v", err)
	}

	err := ValidatePeerRuntimeAuthority(
		configPath,
		filepath.Join(root, "state", "peer.sqlite3"),
		filepath.Join(root, "secrets", "peer.token"),
		filepath.Join(root, "codex"),
		workspaceRoot,
	)
	if err == nil || !strings.Contains(err.Error(), "peer configuration has unexpected hard-link count 2") {
		t.Fatalf("ValidatePeerRuntimeAuthority() error = %v", err)
	}
}

func TestValidatePeerServiceEnvironmentRejectsHardLinkHiddenInWorkspace(t *testing.T) {
	root := t.TempDir()
	environmentPath := filepath.Join(root, "secrets", "peer.env")
	workspaceRoot := filepath.Join(root, "workspaces")
	if err := os.MkdirAll(filepath.Dir(environmentPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(workspaceRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(environmentPath, []byte("GATEWAY_KEY=secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(environmentPath, filepath.Join(workspaceRoot, "provider.env")); err != nil {
		t.Skipf("creating a hard link is unavailable: %v", err)
	}

	err := ValidatePeerServiceEnvironment(
		environmentPath,
		filepath.Join(root, "peer.json"),
		filepath.Join(root, "state", "peer.sqlite3"),
		filepath.Join(root, "secrets", "peer.token"),
		filepath.Join(root, "codex"),
		workspaceRoot,
	)
	if err == nil || !strings.Contains(err.Error(), "peer service environment has unexpected hard-link count 2") {
		t.Fatalf("ValidatePeerServiceEnvironment() error = %v", err)
	}
}

func TestValidateBrokerAuthorityRejectsAliases(t *testing.T) {
	t.Run("case folded master token", func(t *testing.T) {
		root := t.TempDir()
		err := ValidateBrokerAuthority(
			filepath.Join(root, "Config"),
			filepath.Join(root, "broker.sqlite3"),
			filepath.Join(root, "config"),
		)
		if err == nil || !strings.Contains(err.Error(), "master token") {
			t.Fatalf("ValidateBrokerAuthority() error = %v", err)
		}
	})

	t.Run("hard linked state sidecar", func(t *testing.T) {
		root := t.TempDir()
		masterPath := filepath.Join(root, "master.token")
		statePath := filepath.Join(root, "broker.sqlite3")
		if err := os.WriteFile(masterPath, []byte("authority"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(masterPath, statePath+"-wal"); err != nil {
			t.Skipf("creating a hard link is unavailable: %v", err)
		}
		err := ValidateBrokerAuthority(filepath.Join(root, "config.json"), statePath, masterPath)
		if err == nil || !strings.Contains(err.Error(), "broker master token") {
			t.Fatalf("ValidateBrokerAuthority() error = %v", err)
		}
	})

	t.Run("hard linked main database and WAL", func(t *testing.T) {
		root := t.TempDir()
		statePath := filepath.Join(root, "broker.sqlite3")
		if err := os.WriteFile(statePath, []byte("state"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(statePath, statePath+"-wal"); err != nil {
			t.Skipf("creating a hard link is unavailable: %v", err)
		}
		err := ValidateBrokerAuthority(
			filepath.Join(root, "config.json"), statePath, filepath.Join(root, "master.token"),
		)
		if err == nil || !strings.Contains(err.Error(), "broker WAL path conflicts with broker state") {
			t.Fatalf("ValidateBrokerAuthority() error = %v", err)
		}
	})

	t.Run("hard linked master token and instance lease", func(t *testing.T) {
		root := t.TempDir()
		masterPath := filepath.Join(root, "master.token")
		statePath := filepath.Join(root, "broker.sqlite3")
		if err := os.WriteFile(masterPath, []byte("authority"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(masterPath, statePath+".broker.lock"); err != nil {
			t.Skipf("creating a hard link is unavailable: %v", err)
		}
		err := ValidateBrokerAuthority(filepath.Join(root, "config.json"), statePath, masterPath)
		if err == nil || !strings.Contains(err.Error(), "broker instance lease path conflicts with broker master token") {
			t.Fatalf("ValidateBrokerAuthority() error = %v", err)
		}
	})

	t.Run("symlinked state sidecars", func(t *testing.T) {
		root := t.TempDir()
		statePath := filepath.Join(root, "broker.sqlite3")
		journalPath := statePath + "-journal"
		if err := os.WriteFile(journalPath, []byte("journal"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(filepath.Base(journalPath), statePath+"-shm"); err != nil {
			t.Skipf("creating a symbolic link is unavailable: %v", err)
		}
		err := ValidateBrokerAuthority(
			filepath.Join(root, "config.json"), statePath, filepath.Join(root, "master.token"),
		)
		if err == nil || !strings.Contains(err.Error(), "broker shared memory path conflicts with broker rollback journal") {
			t.Fatalf("ValidateBrokerAuthority() error = %v", err)
		}
	})

	t.Run("dangling parent symlink", func(t *testing.T) {
		target := t.TempDir()
		alias := filepath.Join(t.TempDir(), "alias")
		if err := os.Symlink(target, alias); err != nil {
			t.Skipf("creating a directory symlink is unavailable: %v", err)
		}
		err := ValidateBrokerAuthority(
			filepath.Join(alias, "authority"),
			filepath.Join(t.TempDir(), "broker.sqlite3"),
			filepath.Join(target, "authority"),
		)
		if err == nil || !strings.Contains(err.Error(), "master token") {
			t.Fatalf("ValidateBrokerAuthority() error = %v", err)
		}
	})
}

func TestValidateCredentialOutputRejectsAuthorityFiles(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.json")
	statePath := filepath.Join(root, "broker.sqlite3")
	masterPath := filepath.Join(root, "master.token")
	for _, test := range []struct {
		name string
		path string
		want string
	}{
		{name: "configuration", path: configPath, want: "broker configuration"},
		{name: "master token", path: masterPath, want: "broker master token"},
		{name: "state", path: statePath, want: "broker state"},
		{name: "rollback journal", path: statePath + "-journal", want: "broker rollback journal"},
		{name: "WAL", path: statePath + "-wal", want: "broker WAL"},
		{name: "shared memory", path: statePath + "-shm", want: "broker shared memory"},
		{name: "instance lease", path: statePath + ".broker.lock", want: "broker instance lease"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateCredentialOutput(test.path, configPath, statePath, masterPath)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("ValidateCredentialOutput() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestValidateCredentialOutputRejectsHardLinkedBrokerLease(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.json")
	statePath := filepath.Join(root, "broker.sqlite3")
	masterPath := filepath.Join(root, "master.token")
	leasePath := statePath + ".broker.lock"
	outputPath := filepath.Join(root, "device.token")
	if err := os.WriteFile(leasePath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(leasePath, outputPath); err != nil {
		t.Skipf("creating a hard link is unavailable: %v", err)
	}
	err := ValidateCredentialOutput(outputPath, configPath, statePath, masterPath)
	if err == nil || !strings.Contains(err.Error(), "broker instance lease") {
		t.Fatalf("ValidateCredentialOutput() error = %v", err)
	}
}
