package kv

func DeleteRange(s Session, start, end []byte) error {
	it := s.Iterator(start, end)
	defer it.Close()

	for it.First(); it.Valid(); it.Next() {
		err := s.Delete(it.Key())
		if err != nil {
			return err
		}
	}

	return nil
}
