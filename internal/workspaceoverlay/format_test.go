package workspaceoverlay

import (
	"slices"
	"strings"
	"testing"
)

func TestManifestSemanticHashBindsIndexAndWorktreeStates(t *testing.T) {
	payloads := []Payload{
		{SHA256: strings.Repeat("1", 64), Size: 5},
		{SHA256: strings.Repeat("2", 64), Size: 6},
	}
	entries := []Entry{
		{
			Path:     "deleted.txt",
			Worktree: WorktreeState{Kind: NodeAbsent},
		},
		{
			Path: "nested/file.txt",
			Index: &IndexState{
				Mode: "100644", OID: strings.Repeat("a", 40), PayloadSHA256: payloads[0].SHA256,
			},
			Worktree: WorktreeState{
				Kind: NodeFile, Mode: "100755", PayloadSHA256: payloads[1].SHA256,
			},
		},
	}
	manifest, err := NewManifest(strings.Repeat("b", 40), "sha1", "nested", entries, payloads)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.SourceSnapshotHash == zeroSHA256 {
		t.Fatal("semantic hash was not derived from the overlay")
	}
	if err := manifest.VerifySemanticHash(); err != nil {
		t.Fatal(err)
	}
	changed := manifest
	changed.Entries = slices.Clone(manifest.Entries)
	changed.Entries[1].Worktree.Mode = "100644"
	if err := changed.VerifySemanticHash(); err == nil {
		t.Fatal("semantic hash accepted changed worktree state")
	}
}

func TestValidatePathRejectsCrossPlatformAliases(t *testing.T) {
	valid := []string{
		"source/file.go", "space name.txt", "unicode-文件.txt",
		"git~2/config", "git~1x/config", ".gitx/config", ".g\u2069it/config",
	}
	for _, value := range valid {
		if err := ValidatePath(value); err != nil {
			t.Fatalf("ValidatePath(%q) = %v", value, err)
		}
	}
	invalid := []string{
		"", ".", "../escape", "/absolute", `back\\slash`, "drive:name",
		".git/config", "nested/.GIT/index", "git~1/config", "nested/GIT~1/index",
		"trailing. ", "AUX.txt", "com1",
		"line\nbreak", "decomposed-é.txt", `less<than`, `greater>than`, `quote"name`,
		`pipe|name`, `question?name`, `star*name`, "CONIN$", "conout$.txt",
		"COM¹", "lpt².log",
	}
	for _, value := range invalid {
		if err := ValidatePath(value); err == nil {
			t.Fatalf("ValidatePath(%q) unexpectedly succeeded", value)
		}
	}
}

func TestValidatePathRejectsHFSGitAdministrativeAliases(t *testing.T) {
	ignored := []rune{
		'\u200c', '\u200d', '\u200e', '\u200f',
		'\u202a', '\u202b', '\u202c', '\u202d', '\u202e',
		'\u206a', '\u206b', '\u206c', '\u206d', '\u206e', '\u206f',
		'\ufeff',
	}
	for _, character := range ignored {
		for _, component := range []string{
			string(character) + ".Git",
			".g" + string(character) + "It",
			".GiT" + string(character),
		} {
			if err := ValidatePath(component + "/config"); err == nil {
				t.Fatalf("ValidatePath(%q) unexpectedly accepted an HFS .git alias", component)
			}
		}
	}
}

func TestManifestRejectsExpandedPayloadReferenceBomb(t *testing.T) {
	payload := Payload{SHA256: strings.Repeat("1", 64), Size: MaximumPayloadBytes}
	entries := []Entry{
		{
			Path: "a", Worktree: WorktreeState{
				Kind: NodeFile, Mode: "100644", PayloadSHA256: payload.SHA256,
			},
		},
		{
			Path: "b", Index: &IndexState{
				Mode: "100644", OID: strings.Repeat("a", 40), PayloadSHA256: payload.SHA256,
			},
			Worktree: WorktreeState{
				Kind: NodeFile, Mode: "100644", PayloadSHA256: payload.SHA256,
			},
		},
	}
	if _, err := NewManifest(strings.Repeat("b", 40), "sha1", "", entries, []Payload{payload}); err == nil {
		t.Fatal("NewManifest accepted expanded payload references beyond the byte limit")
	}
}

func TestManifestRejectsUnreferencedPayload(t *testing.T) {
	payload := Payload{SHA256: strings.Repeat("1", 64), Size: 1}
	if _, err := NewManifest(strings.Repeat("a", 40), "sha1", "", nil, []Payload{payload}); err == nil {
		t.Fatal("NewManifest accepted an unreferenced payload")
	}
}

func TestManifestRejectsActiveAncestorConflicts(t *testing.T) {
	payload := Payload{SHA256: strings.Repeat("1", 64), Size: 1}
	for _, entries := range [][]Entry{
		{
			{
				Path: "parent", Index: &IndexState{
					Mode: "100644", OID: strings.Repeat("a", 40), PayloadSHA256: payload.SHA256,
				},
				Worktree: WorktreeState{Kind: NodeAbsent},
			},
			{
				Path: "parent/child", Index: &IndexState{
					Mode: "100644", OID: strings.Repeat("b", 40), PayloadSHA256: payload.SHA256,
				},
				Worktree: WorktreeState{Kind: NodeAbsent},
			},
		},
		{
			{
				Path: "parent", Worktree: WorktreeState{
					Kind: NodeFile, Mode: "100644", PayloadSHA256: payload.SHA256,
				},
			},
			{
				Path: "parent/child", Worktree: WorktreeState{
					Kind: NodeFile, Mode: "100644", PayloadSHA256: payload.SHA256,
				},
			},
		},
	} {
		if _, err := NewManifest(strings.Repeat("c", 40), "sha1", "", entries, []Payload{payload}); err == nil {
			t.Fatal("NewManifest accepted an active file ancestor conflict")
		}
	}
}

func TestManifestRejectsPortablePathCollision(t *testing.T) {
	payload := Payload{SHA256: strings.Repeat("1", 64), Size: 1}
	entries := []Entry{
		{
			Path: "Readme", Worktree: WorktreeState{
				Kind: NodeFile, Mode: "100644", PayloadSHA256: payload.SHA256,
			},
		},
		{
			Path: "README", Worktree: WorktreeState{
				Kind: NodeFile, Mode: "100644", PayloadSHA256: payload.SHA256,
			},
		},
	}
	slices.SortFunc(entries, func(left, right Entry) int { return strings.Compare(left.Path, right.Path) })
	if _, err := NewManifest(strings.Repeat("a", 40), "sha1", "", entries, []Payload{payload}); err == nil {
		t.Fatal("NewManifest accepted a case-insensitive path collision")
	}
}

func TestManifestRejectsUnicodeAndCrossStatePathCollisions(t *testing.T) {
	payload := Payload{SHA256: strings.Repeat("1", 64), Size: 1}
	for _, entries := range [][]Entry{
		{
			{
				Path: "Σ.txt", Worktree: WorktreeState{
					Kind: NodeFile, Mode: "100644", PayloadSHA256: payload.SHA256,
				},
			},
			{
				Path: "ς.txt", Worktree: WorktreeState{
					Kind: NodeFile, Mode: "100644", PayloadSHA256: payload.SHA256,
				},
			},
		},
		{
			{
				Path: "README", Index: &IndexState{
					Mode: "100644", OID: strings.Repeat("a", 40), PayloadSHA256: payload.SHA256,
				},
				Worktree: WorktreeState{Kind: NodeAbsent},
			},
			{
				Path: "Readme", Worktree: WorktreeState{
					Kind: NodeFile, Mode: "100644", PayloadSHA256: payload.SHA256,
				},
			},
		},
	} {
		slices.SortFunc(entries, func(left, right Entry) int { return strings.Compare(left.Path, right.Path) })
		if _, err := NewManifest(strings.Repeat("b", 40), "sha1", "", entries, []Payload{payload}); err == nil {
			t.Fatal("NewManifest accepted a cross-platform path collision")
		}
	}
}

func TestManifestRejectsInexactIntentToAddObjectID(t *testing.T) {
	entry := Entry{
		Path: "intent.txt", Index: &IndexState{
			Mode: "100644", OID: strings.Repeat("a", 40), IntentToAdd: true,
		},
		Worktree: WorktreeState{Kind: NodeAbsent},
	}
	if _, err := NewManifest(strings.Repeat("b", 40), "sha1", "", []Entry{entry}, nil); err == nil {
		t.Fatal("NewManifest accepted a non-empty intent-to-add object ID")
	}
	entry.Index.OID = emptyBlobSHA1
	if _, err := NewManifest(strings.Repeat("b", 40), "sha1", "", []Entry{entry}, nil); err != nil {
		t.Fatal(err)
	}
}

func TestManifestAllowsIntentToAddSymlinkWithIndependentWorktreeState(t *testing.T) {
	filePayload := Payload{SHA256: strings.Repeat("1", 64), Size: 1}
	for _, test := range []struct {
		name     string
		worktree WorktreeState
		payloads []Payload
	}{
		{name: "absent", worktree: WorktreeState{Kind: NodeAbsent}},
		{
			name: "regular file",
			worktree: WorktreeState{
				Kind: NodeFile, Mode: "100644", PayloadSHA256: filePayload.SHA256,
			},
			payloads: []Payload{filePayload},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			entry := Entry{
				Path: "intent-link",
				Index: &IndexState{
					Mode: "120000", OID: emptyBlobSHA1, IntentToAdd: true,
				},
				Worktree: test.worktree,
			}
			if _, err := NewManifest(
				strings.Repeat("b", 40), "sha1", "", []Entry{entry}, test.payloads,
			); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestManifestAllowsDeletionBeforeReplacementDescendant(t *testing.T) {
	payload := Payload{SHA256: strings.Repeat("1", 64), Size: 1}
	entries := []Entry{
		{Path: "old", Worktree: WorktreeState{Kind: NodeAbsent}},
		{
			Path: "old/new", Index: &IndexState{
				Mode: "100644", OID: strings.Repeat("a", 40), PayloadSHA256: payload.SHA256,
			},
			Worktree: WorktreeState{Kind: NodeFile, Mode: "100644", PayloadSHA256: payload.SHA256},
		},
	}
	if _, err := NewManifest(strings.Repeat("b", 40), "sha1", "", entries, []Payload{payload}); err != nil {
		t.Fatal(err)
	}
}

func TestManifestAllowsCaseOnlyRenameTombstone(t *testing.T) {
	payload := Payload{SHA256: strings.Repeat("1", 64), Size: 1}
	entries := []Entry{
		{Path: "README", Worktree: WorktreeState{Kind: NodeAbsent}},
		{
			Path: "Readme", Index: &IndexState{
				Mode: "100644", OID: strings.Repeat("a", 40), PayloadSHA256: payload.SHA256,
			},
			Worktree: WorktreeState{Kind: NodeFile, Mode: "100644", PayloadSHA256: payload.SHA256},
		},
	}
	if _, err := NewManifest(strings.Repeat("b", 40), "sha1", "", entries, []Payload{payload}); err != nil {
		t.Fatal(err)
	}
}
