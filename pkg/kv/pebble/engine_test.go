package pebble

//func TestCheckpoint(t *testing.T) {
//	cwd, _ := os.Getwd()
//	root := path.Join(cwd, ".tmp")
//
//	_ = EnsureDirectory(root)
//
//	db, _ := Open(root, nil)
//	t.Cleanup(func() {
//		db.Close()
//	})
//
//	for i := 0; i < 10; i++ {
//		_ = db.Set([]byte("hello"), []byte(fmt.Sprintf("%d", i)), nil)
//	}
//
//	t.Run("create checkpoint", func(t *testing.T) {
//		checkpoint := t.TempDir()
//		t.Cleanup(func() {
//			os.RemoveAll(checkpoint)
//		})
//
//		_ = db.Checkpoint(checkpoint)
//
//		db2, _ := Open(checkpoint, nil)
//		t.Cleanup(func() {
//			db2.Close()
//		})
//
//		data, c, _ := db.Get([]byte("hello"))
//		fmt.Println(string(data))
//		c.Close()
//	})
//}
