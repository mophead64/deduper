package scanner

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/mophead64/deduper/internal/model"
	"github.com/mophead64/deduper/internal/store"
)

func TestRescanGroup(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, n := range []string{"a", "b", "c", "d"} {
		write(n, "same content")
	}

	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	rootID, err := st.AddRoot(ctx, dir, "", model.RootSourceManual)
	if err != nil {
		t.Fatal(err)
	}
	root := model.Root{ID: rootID, Path: dir}
	sc := New(st, DefaultOptions())
	scan := func() {
		id, err := st.StartScan(ctx, []int64{root.ID})
		if err != nil {
			t.Fatal(err)
		}
		if err := sc.Run(ctx, id, []model.Root{root}, nil); err != nil {
			t.Fatal(err)
		}
	}
	scan()

	groups, _, err := st.ListDuplicateGroups(ctx, store.DuplicateFilter{})
	if err != nil || len(groups) != 1 || groups[0].MemberCount != 4 {
		t.Fatalf("setup: groups=%+v err=%v", groups, err)
	}
	gid := groups[0].ID

	// Nothing changed yet.
	res, err := sc.RescanGroup(ctx, gid)
	if err != nil || !res.Exists || res.Unchanged != 4 {
		t.Fatalf("clean rescan: %+v err=%v", res, err)
	}

	os.Remove(filepath.Join(dir, "a"))             // deleted
	write("b", "edited, and a different size now") // modified
	os.Remove(filepath.Join(dir, "c"))             // replaced by a hardlink to d
	if err := os.Link(filepath.Join(dir, "d"), filepath.Join(dir, "c")); err != nil {
		t.Fatal(err)
	}

	res, err = sc.RescanGroup(ctx, gid)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Exists || res.Missing != 1 || res.Changed != 2 {
		t.Fatalf("after cleanup: %+v", res)
	}
	g, err := st.GetDuplicateGroup(ctx, gid)
	if err != nil {
		t.Fatal(err)
	}
	// c and d remain, now hardlinked: 2 paths, 1 instance, nothing reclaimable.
	if g.MemberCount != 2 || g.DistinctInstanceCount != 1 || g.ReclaimableBytes != 0 {
		t.Fatalf("group after rescan: %+v", g)
	}

	os.Remove(filepath.Join(dir, "c"))
	res, err = sc.RescanGroup(ctx, gid)
	if err != nil || res.Exists {
		t.Fatalf("group should dissolve: %+v err=%v", res, err)
	}
	if _, err := st.GetDuplicateGroup(ctx, gid); err == nil {
		t.Fatal("group still exists")
	}
}
