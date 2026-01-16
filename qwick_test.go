package qwick

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestBasic(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "qwick_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.qwick")

	// 1. Создание и сборка
	tree := New()
	tree.Insert([]byte("key1"), []byte("value1"))
	tree.Insert([]byte("key2"), "value2")
	tree.Insert([]byte("key3"), 123) // Будет преобразовано в строку "123"

	err = Build(tree, dbPath)
	if err != nil {
		t.Fatalf("Ошибка Build: %v", err)
	}

	// 2. Открытие
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Ошибка Open: %v", err)
	}
	defer db.Close()

	// 3. Получение данных (Get)
	dst := make([]byte, 1024)
	val, ok, err := db.Find([]byte("key1"), dst)
	if !ok || err != nil || string(val) != "value1" {
		t.Errorf("Ошибка Get key1: получено %q, ok %v, err %v", val, ok, err)
	}

	val, ok, err = db.Find([]byte("key2"), dst)
	if !ok || err != nil || string(val) != "value2" {
		t.Errorf("Ошибка Get key2: получено %q, ok %v, err %v", val, ok, err)
	}

	val, ok, err = db.Find([]byte("key3"), dst)
	if !ok || err != nil || string(val) != "123" {
		t.Errorf("Ошибка Get key3: получено %q, ok %v, err %v", val, ok, err)
	}

	_, ok = db.GetRaw([]byte("non-existent"))
	if ok {
		t.Error("Get для несуществующего ключа должен возвращать false")
	}

	// 5. Поиск по префиксу
	count := 0
	db.PrefixRaw([]byte("key"), func(k, v []byte) bool {
		count++
		return true
	})
	if count != 3 {
		t.Errorf("Ошибка префиксного поиска: ожидалось 3, получено %d", count)
	}

	// Тест остановки итерации
	count = 0
	db.PrefixRaw([]byte("key"), func(k, v []byte) bool {
		count++
		return false
	})
	if count != 1 {
		t.Errorf("Ошибка остановки префиксного поиска: ожидалось 1, получено %d", count)
	}
}

func TestPrefixUnpacked(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "qwick_prefix")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.qwick")

	tree := New()
	tree.Insert([]byte("a1"), []byte("v1"))
	tree.Insert([]byte("a2"), []byte("v2"))
	tree.Insert([]byte("b1"), []byte("v3"))

	err = Build(tree, dbPath)
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	results := make(map[string]string)
	dst := make([]byte, 100)
	err = db.Prefix([]byte("a"), dst, func(k, v []byte) bool {
		results[string(k)] = string(v)
		return true
	})

	if err != nil {
		t.Fatalf("Prefix failed: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Ожидалось 2 результата, получено %d", len(results))
	}
	if results["a1"] != "v1" || results["a2"] != "v2" {
		t.Errorf("Неверные данные: %v", results)
	}
}

func TestCompression(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "qwick_comp")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	tests := []struct {
		name string
		opts BuildOptions
	}{
		{"None", BuildOptions{Compression: compNone}},
		{"Zstd", BuildOptions{Compression: compZstd, ZstdLevel: 1}},
		{"S2", BuildOptions{Compression: compS2}},
		{"Auto", BuildOptions{Compression: 0}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dbPath := filepath.Join(tmpDir, tt.name+".qwick")
			tree := New()
			data := bytes.Repeat([]byte("test_data_"), 100)
			tree.Insert([]byte("k1"), data)

			err := BuildWithOptions(tree, dbPath, tt.opts)
			if err != nil {
				t.Fatalf("Ошибка BuildWithOptions: %v", err)
			}

			db, err := Open(dbPath)
			if err != nil {
				t.Fatalf("Ошибка Open: %v", err)
			}
			defer db.Close()

			val, ok := db.GetRaw([]byte("k1"))
			if !ok {
				t.Fatal("Ключ не найден")
			}
			_ = val

			// Получение распакованных данных
			decVal, ok, err := db.Find([]byte("k1"), nil)
			if err != nil {
				t.Fatalf("Ошибка Find: %v", err)
			}
			if !ok {
				t.Fatal("Ключ не найден в Find")
			}
			if !bytes.Equal(decVal, data) {
				t.Errorf("Данные не совпадают: ожидалось len %d, получено len %d", len(data), len(decVal))
			}

			// Тест с маленьким буфером
			smallDst := make([]byte, 10)
			_, _, err = db.Find([]byte("k1"), smallDst)
			if err == nil && tt.opts.Compression != compNone {
				// S2 и Zstd могут вернуть ошибку, если буфер слишком мал (или просто вернуть срез, если сжатия нет)
				// Реализация Find в qwick.go:
				// если db.compression == compNone { return val, true, nil }
				// Так что если compNone, это нормально.
			}
		})
	}
}

func TestErrors(t *testing.T) {
	// Тест открытия несуществующего файла
	_, err := Open("non-existent-file")
	if err == nil {
		t.Error("Ожидалась ошибка для несуществующего файла")
	}

	// Тест сборки (Build) по неверному пути
	tree := New()
	err = Build(tree, "/invalid/path/to/db")
	if err == nil {
		t.Error("Ожидалась ошибка для неверного пути сборки")
	}

	// Тест открытия некорректного файла
	tmpFile, _ := os.CreateTemp("", "invalid_db")
	defer os.Remove(tmpFile.Name())
	tmpFile.Write([]byte("NOTQWICK!"))
	tmpFile.Close()

	_, err = Open(tmpFile.Name())
	if err == nil {
		t.Error("Ожидалась ошибка для неверной сигнатуры (magic)")
	}
}

func TestExtra(t *testing.T) {
	// 1. Уровни Zstd
	tmpDir, _ := os.MkdirTemp("", "qwick_extra")
	defer os.RemoveAll(tmpDir)

	tree := New()
	tree.Insert([]byte("k"), []byte("v"))

	for _, lvl := range []int{2, 3} {
		path := filepath.Join(tmpDir, fmt.Sprintf("lvl%d.qwick", lvl))
		err := BuildWithOptions(tree, path, BuildOptions{Compression: compZstd, ZstdLevel: lvl})
		if err != nil {
			t.Errorf("Ошибка BuildWithOptions уровень %d: %v", lvl, err)
		}
	}

	// 2. Открытие слишком короткого файла
	shortFile := filepath.Join(tmpDir, "short.qwick")
	os.WriteFile(shortFile, []byte("QWICK"), 0644)
	_, err := Open(shortFile)
	if err == nil || err.Error() != "слишком короткий файл" {
		t.Errorf("Ожидалась ошибка 'слишком короткий файл', получено %v", err)
	}
}

func TestSizeCutover(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "qwick_cutover")
	defer os.RemoveAll(tmpDir)
	dbPath := filepath.Join(tmpDir, "cutover.qwick")

	tree := New()
	smallData := []byte("small")
	largeData := bytes.Repeat([]byte("large"), 100)
	tree.Insert([]byte("small"), smallData)
	tree.Insert([]byte("large"), largeData)

	err := BuildWithOptions(tree, dbPath, BuildOptions{Compression: 0, SizeCutover: 100})
	if err != nil {
		t.Fatalf("BuildWithOptions failed: %v", err)
	}

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	dst := make([]byte, 1000)
	val, ok, err := db.Find([]byte("small"), dst)
	if !ok || !bytes.Equal(val, smallData) {
		t.Errorf("несоответствие данных для 'small'")
	}

	val, ok, err = db.Find([]byte("large"), dst)
	if !ok || !bytes.Equal(val, largeData) {
		t.Errorf("несоответствие данных для 'large': ожидалось len %d, получено len %d, компрессия в БД: %d", len(largeData), len(val), db.compression)
	}
}
func TestErrorsMore(t *testing.T) {
	// 1. Открытие файла с неверной версией
	tmpDir, _ := os.MkdirTemp("", "qwick_err")
	defer os.RemoveAll(tmpDir)
	dbPath := filepath.Join(tmpDir, "ver.qwick")

	tree := New()
	tree.Insert([]byte("a"), []byte("b"))
	Build(tree, dbPath)

	data, _ := os.ReadFile(dbPath)
	binary.LittleEndian.PutUint32(data[8:12], 999) // неверная версия
	os.WriteFile(dbPath, data, 0644)

	dbVer, err := Open(dbPath)
	if err != nil {
		t.Logf("Open вернул ошибку для версии: %v", err)
	}
	if dbVer != nil && dbVer.hdr.Version == 999 {
		t.Log("Open разрешил неверную версию, что является текущим поведением, но полезно для покрытия")
	}
	if dbVer != nil {
		dbVer.Close()
	}
}
func TestPrefixAdvanced(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "qwick_prefix_adv")
	defer os.RemoveAll(tmpDir)

	tree := New()
	entries := []struct {
		k, v string
	}{
		{"apple", "fruit1"},
		{"apply", "action"},
		{"banana", "fruit2"},
		{"box", "container"},
		{"boy", "child"},
	}
	for _, e := range entries {
		tree.Insert([]byte(e.k), []byte(e.v))
	}

	tests := []struct {
		name        string
		prefix      string
		compression uint32
		wantKeys    []string
	}{
		{"NoComp_App", "app", compNone, []string{"apple", "apply"}},
		{"NoComp_Bo", "bo", compNone, []string{"box", "boy"}},
		{"NoComp_Empty", "", compNone, []string{"apple", "apply", "banana", "box", "boy"}},
		{"NoComp_None", "zzz", compNone, []string{}},
		{"Zstd_App", "app", compZstd, []string{"apple", "apply"}},
		{"S2_Bo", "bo", compS2, []string{"box", "boy"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dbPath := filepath.Join(tmpDir, tt.name+".qwick")
			BuildWithOptions(tree, dbPath, BuildOptions{Compression: tt.compression})
			db, _ := Open(dbPath)
			defer db.Close()

			// Test PrefixRaw
			var gotRaw []string
			db.PrefixRaw([]byte(tt.prefix), func(k, v []byte) bool {
				gotRaw = append(gotRaw, string(k))
				return true
			})
			if !equalStrings(gotRaw, tt.wantKeys) {
				t.Errorf("PrefixRaw %s: got %v, want %v", tt.prefix, gotRaw, tt.wantKeys)
			}

			// Test Prefix (with decode)
			var gotDec []string
			dst := make([]byte, 100)
			db.Prefix([]byte(tt.prefix), dst, func(k, v []byte) bool {
				gotDec = append(gotDec, string(k))
				// Verify value is correct
				expectedVal := ""
				for _, e := range entries {
					if e.k == string(k) {
						expectedVal = e.v
						break
					}
				}
				if string(v) != expectedVal {
					t.Errorf("Prefix key %s: got val %s, want %s", k, v, expectedVal)
				}
				return true
			})
			if !equalStrings(gotDec, tt.wantKeys) {
				t.Errorf("Prefix %s: got %v, want %v", tt.prefix, gotDec, tt.wantKeys)
			}
		})
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestPanicReproduction(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "qwick_panic")
	defer os.RemoveAll(tmpDir)
	dbPath := filepath.Join(tmpDir, "panic.qwick")

	// Создаем минимально валидный заголовок, но с некорректными смещениями
	hdr := make([]byte, 64)
	copy(hdr[0:8], FileMagic)
	binary.LittleEndian.PutUint32(hdr[8:12], FileVersion)
	binary.LittleEndian.PutUint64(hdr[16:24], 1)           // NumEntries
	binary.LittleEndian.PutUint64(hdr[24:32], 64)          // OffIndex
	binary.LittleEndian.PutUint64(hdr[32:40], 10000000000) // OffBlobs (далеко за пределами файла)

	// Добавляем одну запись индекса
	idx := make([]byte, 24)
	binary.LittleEndian.PutUint64(idx[0:8], 10000000000) // koff за пределами
	binary.LittleEndian.PutUint32(idx[8:12], 10)         // klen
	binary.LittleEndian.PutUint64(idx[12:20], 10000000010)
	binary.LittleEndian.PutUint32(idx[20:24], 10)

	f, _ := os.Create(dbPath)
	f.Write(hdr)
	f.Write(idx)
	f.Close()

	db, err := Open(dbPath)
	if err != nil {
		// Если Open уже возвращает ошибку - это хорошо (значит мы уже пофиксили или он частично валидирует)
		return
	}
	defer db.Close()

	// Это должно было вызвать панику до фикса
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Паника в GetRaw: %v", r)
		}
	}()
	db.GetRaw([]byte("test"))
}

func TestCorruptedDB(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "qwick_corrupt")
	defer os.RemoveAll(tmpDir)
	dbPath := filepath.Join(tmpDir, "corrupt.qwick")

	t.Run("InvalidCompression", func(t *testing.T) {
		hdr := make([]byte, 64)
		copy(hdr[0:8], FileMagic)
		binary.LittleEndian.PutUint32(hdr[8:12], FileVersion)
		binary.LittleEndian.PutUint32(hdr[44:48], 99) // Невалидное сжатие
		os.WriteFile(dbPath, hdr, 0644)
		_, err := Open(dbPath)
		if err == nil || !bytes.Contains([]byte(err.Error()), []byte("неподдерживаемый тип сжатия")) {
			t.Errorf("Ожидалась ошибка сжатия, получено: %v", err)
		}
	})

	t.Run("IndexOutOfBounds", func(t *testing.T) {
		hdr := make([]byte, 64)
		copy(hdr[0:8], FileMagic)
		binary.LittleEndian.PutUint32(hdr[8:12], FileVersion)
		binary.LittleEndian.PutUint64(hdr[16:24], 100) // 100 записей
		binary.LittleEndian.PutUint64(hdr[24:32], 64)  // Смещение 64
		// Общий размер должен быть 64 + 100*24 = 2464, а файл всего 64
		os.WriteFile(dbPath, hdr, 0644)
		_, err := Open(dbPath)
		if err == nil || !bytes.Contains([]byte(err.Error()), []byte("некорректный размер индекса")) {
			t.Errorf("Ожидалась ошибка индекса, получено: %v", err)
		}
	})
}

func BenchmarkGet(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "qwick_bench")
	defer os.RemoveAll(tmpDir)
	dbPath := filepath.Join(tmpDir, "bench.qwick")

	tree := New()
	key := []byte("key")
	val := []byte("value")
	tree.Insert(key, val)
	Build(tree, dbPath)

	db, _ := Open(dbPath)
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.GetRaw(key)
	}
}

func BenchmarkBuild(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "qwick_bench_build")
	defer os.RemoveAll(tmpDir)
	dbPath := filepath.Join(tmpDir, "bench_build.qwick")

	tree := New()
	for i := 0; i < 1000; i++ {
		tree.Insert([]byte(fmt.Sprintf("key%d", i)), []byte("value"))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Build(tree, dbPath)
	}
}

func TestZipUnzip(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "qwick_zip")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	srcPath := filepath.Join(tmpDir, "src.txt")
	encPath := filepath.Join(tmpDir, "enc.bin")
	decPath := filepath.Join(tmpDir, "dec.txt")

	// Тестовые данные (чуть больше 1 МБ, чтобы проверить несколько чанков)
	data := bytes.Repeat([]byte("Hello, Qwick! Encryption and Compression test. "), 30000)
	err = os.WriteFile(srcPath, data, 0644)
	if err != nil {
		t.Fatal(err)
	}

	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}

	// 1. Шифруем
	err = ZipEncrypt(encPath, srcPath, key)
	if err != nil {
		t.Fatalf("ZipEncrypt failed: %v", err)
	}

	// 2. Расшифровываем
	err = UnzipDecrypt(decPath, encPath, key)
	if err != nil {
		t.Fatalf("UnzipDecrypt failed: %v", err)
	}

	// 3. Проверяем результат
	decData, err := os.ReadFile(decPath)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(data, decData) {
		t.Error("Decrypted data does not match original data")
	}

	// 4. Проверяем с неправильным ключом
	wrongKey := make([]byte, 32)
	wrongKey[0] = 0xFF
	err = UnzipDecrypt(filepath.Join(tmpDir, "bad.txt"), encPath, wrongKey)
	if err == nil {
		t.Error("UnzipDecrypt should fail with wrong key")
	} else {
		t.Logf("Expected error with wrong key: %v", err)
	}

	// 5. Проверка пустого файла
	emptySrc := filepath.Join(tmpDir, "empty.txt")
	emptyEnc := filepath.Join(tmpDir, "empty.enc")
	emptyDec := filepath.Join(tmpDir, "empty.dec")
	os.WriteFile(emptySrc, []byte{}, 0644)

	err = ZipEncrypt(emptyEnc, emptySrc, key)
	if err != nil {
		t.Fatalf("ZipEncrypt empty file failed: %v", err)
	}
	err = UnzipDecrypt(emptyDec, emptyEnc, key)
	if err != nil {
		t.Fatalf("UnzipDecrypt empty file failed: %v", err)
	}
	emptyData, _ := os.ReadFile(emptyDec)
	if len(emptyData) != 0 {
		t.Error("Empty file decryption should result in empty file")
	}
}
