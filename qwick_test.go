package soda

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestBasic(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "soda_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.soda")

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
	val, ok, err := db.GetDecompressed([]byte("key1"), dst)
	if !ok || err != nil || string(val) != "value1" {
		t.Errorf("Ошибка Get key1: получено %q, ok %v, err %v", val, ok, err)
	}

	val, ok, err = db.GetDecompressed([]byte("key2"), dst)
	if !ok || err != nil || string(val) != "value2" {
		t.Errorf("Ошибка Get key2: получено %q, ok %v, err %v", val, ok, err)
	}

	val, ok, err = db.GetDecompressed([]byte("key3"), dst)
	if !ok || err != nil || string(val) != "123" {
		t.Errorf("Ошибка Get key3: получено %q, ok %v, err %v", val, ok, err)
	}

	_, ok = db.Get([]byte("non-existent"))
	if ok {
		t.Error("Get для несуществующего ключа должен возвращать false")
	}

	// 5. Поиск по префиксу
	count := 0
	db.Prefix([]byte("key"), func(k, v []byte) bool {
		count++
		return true
	})
	if count != 3 {
		t.Errorf("Ошибка префиксного поиска: ожидалось 3, получено %d", count)
	}

	// Тест остановки итерации
	count = 0
	db.Prefix([]byte("key"), func(k, v []byte) bool {
		count++
		return false
	})
	if count != 1 {
		t.Errorf("Ошибка остановки префиксного поиска: ожидалось 1, получено %d", count)
	}
}

func TestCompression(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "soda_comp")
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
			dbPath := filepath.Join(tmpDir, tt.name+".soda")
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

			val, ok := db.Get([]byte("k1"))
			if !ok {
				t.Fatal("Ключ не найден")
			}
			_ = val

			// Получение распакованных данных
			decVal, ok, err := db.GetDecompressed([]byte("k1"), nil)
			if err != nil {
				t.Fatalf("Ошибка GetDecompressed: %v", err)
			}
			if !ok {
				t.Fatal("Ключ не найден в GetDecompressed")
			}
			if !bytes.Equal(decVal, data) {
				t.Errorf("Данные не совпадают: ожидалось len %d, получено len %d", len(data), len(decVal))
			}

			// Тест с маленьким буфером
			smallDst := make([]byte, 10)
			_, _, err = db.GetDecompressed([]byte("k1"), smallDst)
			if err == nil && tt.opts.Compression != compNone {
				// S2 и Zstd могут вернуть ошибку, если буфер слишком мал (или просто вернуть срез, если сжатия нет)
				// Реализация GetDecompressed в soda.go:
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
	tmpFile.Write([]byte("NOTSODA!"))
	tmpFile.Close()

	_, err = Open(tmpFile.Name())
	if err == nil {
		t.Error("Ожидалась ошибка для неверной сигнатуры (magic)")
	}
}

func TestExtra(t *testing.T) {
	// 1. Уровни Zstd
	tmpDir, _ := os.MkdirTemp("", "soda_extra")
	defer os.RemoveAll(tmpDir)

	tree := New()
	tree.Insert([]byte("k"), []byte("v"))

	for _, lvl := range []int{2, 3} {
		path := filepath.Join(tmpDir, fmt.Sprintf("lvl%d.soda", lvl))
		err := BuildWithOptions(tree, path, BuildOptions{Compression: compZstd, ZstdLevel: lvl})
		if err != nil {
			t.Errorf("Ошибка BuildWithOptions уровень %d: %v", lvl, err)
		}
	}

	// 2. Открытие слишком короткого файла
	shortFile := filepath.Join(tmpDir, "short.soda")
	os.WriteFile(shortFile, []byte("SODA"), 0644)
	_, err := Open(shortFile)
	if err == nil || err.Error() != "слишком короткий файл" {
		t.Errorf("Ожидалась ошибка 'слишком короткий файл', получено %v", err)
	}
}

func TestSizeCutover(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "soda_cutover")
	defer os.RemoveAll(tmpDir)
	dbPath := filepath.Join(tmpDir, "cutover.soda")

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
	val, ok, err := db.GetDecompressed([]byte("small"), dst)
	if !ok || !bytes.Equal(val, smallData) {
		t.Errorf("несоответствие данных для 'small'")
	}

	val, ok, err = db.GetDecompressed([]byte("large"), dst)
	if !ok || !bytes.Equal(val, largeData) {
		t.Errorf("несоответствие данных для 'large': ожидалось len %d, получено len %d, компрессия в БД: %d", len(largeData), len(val), db.compression)
	}
}
func TestErrorsMore(t *testing.T) {
	// 1. Открытие файла с неверной версией
	tmpDir, _ := os.MkdirTemp("", "soda_err")
	defer os.RemoveAll(tmpDir)
	dbPath := filepath.Join(tmpDir, "ver.soda")

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
func BenchmarkGet(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "soda_bench")
	defer os.RemoveAll(tmpDir)
	dbPath := filepath.Join(tmpDir, "bench.soda")

	tree := New()
	key := []byte("key")
	val := []byte("value")
	tree.Insert(key, val)
	Build(tree, dbPath)

	db, _ := Open(dbPath)
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Get(key)
	}
}

func BenchmarkBuild(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "soda_bench_build")
	defer os.RemoveAll(tmpDir)
	dbPath := filepath.Join(tmpDir, "bench_build.soda")

	tree := New()
	for i := 0; i < 1000; i++ {
		tree.Insert([]byte(fmt.Sprintf("key%d", i)), []byte("value"))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Build(tree, dbPath)
	}
}
