package qwick

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/edsrzf/mmap-go"
	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
	art "github.com/plar/go-adaptive-radix-tree/v2"
)

// Константы формата файла QWICK
const (
	FileMagic   = "QWICK2026"
	FileVersion = 1
	headerSize  = 64
)

// Типы сжатия
const (
	compNone = 0
	compZstd = 1
	compS2   = 2
)

// indexEntrySize - размер одной записи индекса (24 байта).
const indexEntrySize = uint64(8 + 4 + 8 + 4)

// fileHeader представляет заголовок файла на диске.
type fileHeader struct {
	Magic       [8]byte
	Version     uint32
	_           uint32 // padding
	NumEntries  uint64
	OffIndex    uint64
	OffBlobs    uint64
	ValueFmt    uint32 // 100 = generic
	Compression uint32 // 0 = none, 1 = zstd, 2 = s2
}

// MMAPDB представляет собой базу данных с доступом через memory-mapped file (только для чтения).
type MMAPDB struct {
	mdata       mmap.MMap
	hdr         fileHeader
	indexBase   uint64
	indexSize   uint64
	num         uint64
	compression uint32
}

// Глобальный zstd-декодер для быстрой распаковки
var zstdDec, _ = zstd.NewReader(nil)

// New создаёт новое адаптивное радикс-дерево (ART) в памяти.
func New() art.Tree {
	return art.New()
}

// Open открывает базу данных из указанного пути.
func Open(path string) (*MMAPDB, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	m, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		return nil, err
	}

	if len(m) < int(headerSize) {
		_ = m.Unmap()
		return nil, errors.New("слишком короткий файл")
	}

	var hdr fileHeader
	copy(hdr.Magic[:], m[0:8])
	if string(hdr.Magic[:]) != FileMagic {
		_ = m.Unmap()
		return nil, errors.New("неверная сигнатура файла (magic)")
	}

	hdr.Version = binary.LittleEndian.Uint32(m[8:12])
	hdr.NumEntries = binary.LittleEndian.Uint64(m[16:24])
	hdr.OffIndex = binary.LittleEndian.Uint64(m[24:32])
	hdr.OffBlobs = binary.LittleEndian.Uint64(m[32:40])
	hdr.ValueFmt = binary.LittleEndian.Uint32(m[40:44])
	hdr.Compression = binary.LittleEndian.Uint32(m[44:48])

	db := &MMAPDB{
		mdata:       m,
		hdr:         hdr,
		indexBase:   hdr.OffIndex,
		indexSize:   indexEntrySize,
		num:         hdr.NumEntries,
		compression: hdr.Compression,
	}

	return db, nil
}

// Close закрывает базу данных и освобождает mmap.
func (db *MMAPDB) Close() error {
	return db.mdata.Unmap()
}

// Get выполняет поиск ключа и возвращает сырые данные (указывает прямо в mmap).
func (db *MMAPDB) GetRaw(key []byte) ([]byte, bool) {
	idx, ok := db.findIndex(key)
	if !ok {
		return nil, false
	}
	_, _, voff, vlen := db.readIndex(idx)
	return db.mdata[voff : voff+uint64(vlen)], true
}

// Find возвращает распакованное значение в dst.
func (db *MMAPDB) Find(key []byte, dst []byte) ([]byte, bool, error) {
	val, ok := db.GetRaw(key)
	if !ok {
		return nil, false, nil
	}
	switch db.compression {
	case compZstd:
		out, err := zstdDec.DecodeAll(val, dst[:0])
		return out, true, err
	case compS2:
		out, err := s2.Decode(dst[:0], val)
		return out, true, err
	case 0:
		// Авто-режим: пробуем S2 первым, потом Zstd.
		out, err := s2.Decode(dst[:0], val)
		if err == nil {
			return out, true, nil
		}
		out, err = zstdDec.DecodeAll(val, dst[:0])
		if err == nil {
			return out, true, nil
		}
		return val, true, nil
	default:
		return val, true, nil
	}
}

// Prefix перебирает все ключи, начинающиеся с prefix.
func (db *MMAPDB) Prefix(prefix []byte, cb func(key, val []byte) bool) {
	idx, _ := db.findIndex(prefix)
	for i := idx; i < db.num; i++ {
		k := db.getKeySlice(i)
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		if !cb(k, db.getValSlice(i)) {
			break
		}
	}
}

// findIndex выполняет бинарный поиск индекса по ключу.
func (db *MMAPDB) findIndex(key []byte) (uint64, bool) {
	var lo, hi uint64 = 0, db.num
	for lo < hi {
		mid := (lo + hi) >> 1
		k := db.getKeySlice(mid)
		cmp := bytes.Compare(k, key)
		if cmp == 0 {
			return mid, true
		} else if cmp < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo, false
}

func (db *MMAPDB) readIndex(i uint64) (koff uint64, klen uint32, voff uint64, vlen uint32) {
	off := db.indexBase + i*indexEntrySize
	koff = binary.LittleEndian.Uint64(db.mdata[off : off+8])
	klen = binary.LittleEndian.Uint32(db.mdata[off+8 : off+12])
	voff = binary.LittleEndian.Uint64(db.mdata[off+12 : off+20])
	vlen = binary.LittleEndian.Uint32(db.mdata[off+20 : off+24])
	return
}

func (db *MMAPDB) getKeySlice(i uint64) []byte {
	koff, klen, _, _ := db.readIndex(i)
	return db.mdata[koff : koff+uint64(klen)]
}

func (db *MMAPDB) getValSlice(i uint64) []byte {
	_, _, voff, vlen := db.readIndex(i)
	return db.mdata[voff : voff+uint64(vlen)]
}

// BuildOptions управляет настройками компрессии при сборке базы.
type BuildOptions struct {
	Compression uint32 // 0=auto, 1=zstd, 2=s2
	ZstdLevel   int    // 1..3 уровни скорости
	SizeCutover int    // порог выбора между s2 и zstd для режима auto
}

// BuildWithOptions сериализует ART дерево в файл с заданными опциями.
func BuildWithOptions(tree art.Tree, path string, opts BuildOptions) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("ошибка создания директории %s: %w", dir, err)
	}

	var num uint64
	tree.ForEach(func(n art.Node) (cont bool) {
		num++
		return true
	}, art.TraverseLeaf)

	offIndex := uint64(headerSize)
	indexSize := num * indexEntrySize
	offBlobs := offIndex + indexSize

	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("ошибка создания временного файла: %w", err)
	}
	defer f.Close()

	if _, err := f.Write(make([]byte, headerSize)); err != nil {
		return fmt.Errorf("ошибка записи заглушки заголовка: %w", err)
	}

	if _, err := f.Seek(int64(offBlobs), io.SeekStart); err != nil {
		return fmt.Errorf("ошибка перехода к области данных: %w", err)
	}

	const alignTo = 8

	type idx struct {
		koff uint64
		klen uint32
		voff uint64
		vlen uint32
	}
	indices := make([]idx, 0, num)

	compression := opts.Compression
	if compression == 0 {
		// Режим авто - по умолчанию S2, но для конкретных блоков может быть Zstd
		compression = 0
	}

	var zenc *zstd.Encoder
	if compression == compZstd {
		level := zstd.SpeedFastest
		if opts.ZstdLevel == 2 {
			level = zstd.SpeedDefault
		} else if opts.ZstdLevel == 3 {
			level = zstd.SpeedBetterCompression
		}
		zenc, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(level))
		defer zenc.Close()
	}

	tree.ForEach(func(n art.Node) (cont bool) {
		k := n.Key()
		v := n.Value()
		var vb []byte
		switch vv := v.(type) {
		case []byte:
			vb = vv
		case string:
			vb = []byte(vv)
		default:
			vb = []byte(fmt.Sprint(vv))
		}

		off, _ := f.Seek(0, io.SeekCurrent)
		koff := uint64(off)
		_, _ = f.Write(k)
		klen := uint32(len(k))

		off2, _ := f.Seek(0, io.SeekCurrent)
		voff := uint64(off2)

		var cv []byte
		compToUse := compression
		if opts.Compression == 0 && opts.SizeCutover > 0 {
			if len(vb) > opts.SizeCutover {
				compToUse = compZstd
			} else {
				compToUse = compS2
			}
		}

		switch compToUse {
		case compZstd:
			// Для Zstd в режиме авто создадим энкодер если его нет
			if zenc == nil {
				zenc, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
				defer zenc.Close()
			}
			cv = zenc.EncodeAll(vb, nil)
		case compS2:
			cv = s2.Encode(nil, vb)
		default:
			cv = vb
		}
		_, _ = f.Write(cv)
		vlen := uint32(len(cv))

		indices = append(indices, idx{koff, klen, voff, vlen})
		return true
	}, art.TraverseLeaf)

	_, _ = f.Seek(int64(offIndex), io.SeekStart)
	recBuf := make([]byte, indexEntrySize)
	for _, it := range indices {
		binary.LittleEndian.PutUint64(recBuf[0:8], it.koff)
		binary.LittleEndian.PutUint32(recBuf[8:12], it.klen)
		binary.LittleEndian.PutUint64(recBuf[12:20], it.voff)
		binary.LittleEndian.PutUint32(recBuf[20:24], it.vlen)
		_, _ = f.Write(recBuf)
	}

	_, _ = f.Seek(0, io.SeekStart)
	hdrBuf := make([]byte, headerSize)
	copy(hdrBuf[0:8], []byte(FileMagic))
	binary.LittleEndian.PutUint32(hdrBuf[8:12], FileVersion)
	binary.LittleEndian.PutUint64(hdrBuf[16:24], num)
	binary.LittleEndian.PutUint64(hdrBuf[24:32], offIndex)
	binary.LittleEndian.PutUint64(hdrBuf[32:40], offBlobs)
	binary.LittleEndian.PutUint32(hdrBuf[40:44], 100)
	binary.LittleEndian.PutUint32(hdrBuf[44:48], compression)
	_, _ = f.Write(hdrBuf)

	_ = f.Sync()
	_ = f.Close()
	return os.Rename(tmp, path)
}

// Build — обёртка над BuildWithOptions с параметрами по умолчанию.
func Build(tree art.Tree, path string) error {
	return BuildWithOptions(tree, path, BuildOptions{Compression: 0, ZstdLevel: 1, SizeCutover: 256})
}
