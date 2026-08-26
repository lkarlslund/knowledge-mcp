package kiwix

import (
	"bytes"
	"compress/bzip2"
	"compress/zlib"
	"crypto/sha256"
	"encoding/base32"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/ulikunitz/xz"
)

const (
	zimMagic        = 0x044d495a
	zimHeaderBytes  = 80
	maximumDirent   = 1 << 20
	clusterCacheMax = 8
)

type zimHeader struct {
	Major, Minor                           uint16
	EntryCount, ClusterCount               uint32
	PathPtrPos, TitlePtrPos, ClusterPtrPos uint64
	MimeListPos, ChecksumPos               uint64
}

type zimEntry struct {
	Index       uint32
	MIME        uint16
	Namespace   byte
	Cluster     uint32
	Blob        uint32
	Redirect    uint32
	Path, Title string
}

func (entry zimEntry) isRedirect() bool { return entry.MIME == 0xffff }

type zimArchive struct {
	file       *os.File
	header     zimHeader
	mimes      []string
	cacheMu    sync.Mutex
	cache      map[uint32][]byte
	cacheOrder []uint32
}

func openZIM(path string) (*zimArchive, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	archive := &zimArchive{file: file, cache: make(map[uint32][]byte)}
	if err := archive.readHeader(); err != nil {
		_ = file.Close()
		return nil, err
	}
	mimes, err := archive.readMIMEs()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	archive.mimes = mimes
	return archive, nil
}

func (archive *zimArchive) Close() error { return archive.file.Close() }

func (archive *zimArchive) readHeader() error {
	data := make([]byte, zimHeaderBytes)
	if _, err := archive.file.ReadAt(data, 0); err != nil {
		return fmt.Errorf("read ZIM header: %w", err)
	}
	if binary.LittleEndian.Uint32(data) != zimMagic {
		return errors.New("invalid ZIM magic number")
	}
	header := zimHeader{
		Major: binary.LittleEndian.Uint16(data[4:]), Minor: binary.LittleEndian.Uint16(data[6:]),
		EntryCount: binary.LittleEndian.Uint32(data[24:]), ClusterCount: binary.LittleEndian.Uint32(data[28:]),
		PathPtrPos: binary.LittleEndian.Uint64(data[32:]), TitlePtrPos: binary.LittleEndian.Uint64(data[40:]),
		ClusterPtrPos: binary.LittleEndian.Uint64(data[48:]), MimeListPos: binary.LittleEndian.Uint64(data[56:]),
		ChecksumPos: binary.LittleEndian.Uint64(data[72:]),
	}
	if header.Major < 5 || header.Major > 6 || header.MimeListPos < zimHeaderBytes || header.ChecksumPos <= header.MimeListPos {
		return fmt.Errorf("unsupported or corrupt ZIM version %d.%d", header.Major, header.Minor)
	}
	archive.header = header
	return nil
}

func (archive *zimArchive) readMIMEs() ([]string, error) {
	limit := min(archive.header.ChecksumPos, archive.header.MimeListPos+(4<<20))
	data := make([]byte, limit-archive.header.MimeListPos)
	if _, err := archive.file.ReadAt(data, int64(archive.header.MimeListPos)); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	var result []string
	for len(data) > 0 {
		end := bytes.IndexByte(data, 0)
		if end < 0 {
			return nil, errors.New("unterminated ZIM MIME list")
		}
		if end == 0 {
			return result, nil
		}
		result = append(result, string(data[:end]))
		data = data[end+1:]
	}
	return nil, errors.New("unterminated ZIM MIME list")
}

func (archive *zimArchive) entry(index uint32) (zimEntry, error) {
	if index >= archive.header.EntryCount {
		return zimEntry{}, fmt.Errorf("ZIM entry %d is out of range", index)
	}
	var pointer [8]byte
	if _, err := archive.file.ReadAt(pointer[:], int64(archive.header.PathPtrPos)+int64(index)*8); err != nil {
		return zimEntry{}, err
	}
	offset := binary.LittleEndian.Uint64(pointer[:])
	buffer, err := archive.readDirentBytes(offset)
	if err != nil {
		return zimEntry{}, err
	}
	if len(buffer) < 12 {
		return zimEntry{}, errors.New("truncated ZIM directory entry")
	}
	entry := zimEntry{Index: index, MIME: binary.LittleEndian.Uint16(buffer), Namespace: buffer[3]}
	headerSize := 12
	if entry.isRedirect() {
		entry.Redirect = binary.LittleEndian.Uint32(buffer[8:])
	} else {
		if len(buffer) < 16 {
			return zimEntry{}, errors.New("truncated ZIM content entry")
		}
		entry.Cluster, entry.Blob, headerSize = binary.LittleEndian.Uint32(buffer[8:]), binary.LittleEndian.Uint32(buffer[12:]), 16
	}
	path, rest, ok := takeCString(buffer[headerSize:])
	if !ok {
		return zimEntry{}, errors.New("unterminated ZIM entry path")
	}
	title, _, ok := takeCString(rest)
	if !ok {
		return zimEntry{}, errors.New("unterminated ZIM entry title")
	}
	entry.Path, entry.Title = path, title
	if entry.Title == "" {
		entry.Title = entry.Path
	}
	return entry, nil
}

func (archive *zimArchive) readDirentBytes(offset uint64) ([]byte, error) {
	for size := 4 << 10; size <= maximumDirent; size *= 2 {
		buffer := make([]byte, size)
		n, err := archive.file.ReadAt(buffer, int64(offset))
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		buffer = buffer[:n]
		minimum := 12
		if len(buffer) >= 2 && binary.LittleEndian.Uint16(buffer) != 0xffff {
			minimum = 16
		}
		if len(buffer) >= minimum {
			first := bytes.IndexByte(buffer[minimum:], 0)
			if first >= 0 && bytes.IndexByte(buffer[minimum+first+1:], 0) >= 0 {
				return buffer, nil
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
	}
	return nil, errors.New("ZIM directory entry exceeds safety limit")
}

func takeCString(data []byte) (string, []byte, bool) {
	end := bytes.IndexByte(data, 0)
	if end < 0 {
		return "", nil, false
	}
	return string(data[:end]), data[end+1:], true
}

func (archive *zimArchive) mime(entry zimEntry) string {
	if int(entry.MIME) >= len(archive.mimes) {
		return ""
	}
	return archive.mimes[entry.MIME]
}

func (archive *zimArchive) blob(entry zimEntry) ([]byte, error) {
	if entry.isRedirect() {
		return nil, errors.New("redirect has no content blob")
	}
	data, err := archive.cluster(entry.Cluster)
	if err != nil {
		return nil, err
	}
	var width uint64 = 4
	if len(data) == 0 {
		return nil, errors.New("empty ZIM cluster")
	}
	// cluster() returns decompressed bytes after the information byte. The
	// extended bit is retained as a sentinel prefix for offset decoding.
	if data[0] == 1 {
		width, data = 8, data[1:]
	} else {
		data = data[1:]
	}
	readOffset := func(number uint32) (uint64, error) {
		position := uint64(number) * width
		if position+width > uint64(len(data)) {
			return 0, errors.New("ZIM blob offset is out of range")
		}
		if width == 8 {
			return binary.LittleEndian.Uint64(data[position:]), nil
		}
		return uint64(binary.LittleEndian.Uint32(data[position:])), nil
	}
	start, err := readOffset(entry.Blob)
	if err != nil {
		return nil, err
	}
	end, err := readOffset(entry.Blob + 1)
	if err != nil || start > end || end > uint64(len(data)) {
		return nil, errors.New("invalid ZIM blob bounds")
	}
	return append([]byte(nil), data[start:end]...), nil
}

func (archive *zimArchive) cluster(number uint32) ([]byte, error) {
	archive.cacheMu.Lock()
	defer archive.cacheMu.Unlock()
	if data := archive.cache[number]; data != nil {
		return data, nil
	}
	if number >= archive.header.ClusterCount {
		return nil, fmt.Errorf("ZIM cluster %d is out of range", number)
	}
	var pointers [16]byte
	need := 16
	if number+1 == archive.header.ClusterCount {
		need = 8
	}
	if _, err := archive.file.ReadAt(pointers[:need], int64(archive.header.ClusterPtrPos)+int64(number)*8); err != nil {
		return nil, err
	}
	start := binary.LittleEndian.Uint64(pointers[:8])
	end := archive.header.ChecksumPos
	if need == 16 {
		end = binary.LittleEndian.Uint64(pointers[8:])
	}
	if end <= start || end-start > 1<<34 {
		return nil, errors.New("invalid ZIM cluster bounds")
	}
	raw := make([]byte, end-start)
	if _, err := archive.file.ReadAt(raw, int64(start)); err != nil {
		return nil, err
	}
	if len(raw) < 1 {
		return nil, errors.New("truncated ZIM cluster")
	}
	info, payload := raw[0], raw[1:]
	compression := info & 0x0f
	var reader io.ReadCloser
	switch compression {
	case 0, 1:
		reader = io.NopCloser(bytes.NewReader(payload))
	case 2:
		zlibReader, err := zlib.NewReader(bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		reader = zlibReader
	case 3:
		reader = io.NopCloser(bzip2.NewReader(bytes.NewReader(payload)))
	case 4:
		xzReader, err := xz.NewReader(bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		reader = io.NopCloser(xzReader)
	case 5:
		decoder, err := zstd.NewReader(bytes.NewReader(payload), zstd.WithDecoderConcurrency(1))
		if err != nil {
			return nil, err
		}
		reader = decoder.IOReadCloser()
	default:
		return nil, fmt.Errorf("unsupported ZIM cluster compression %d", compression)
	}
	decoded, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return nil, err
	}
	// Prefix encodes only the extended bit; the original compression byte is
	// not part of the uncompressed cluster offset table.
	prefix := byte(0)
	if info&0x10 != 0 {
		prefix = 1
	}
	decoded = append([]byte{prefix}, decoded...)
	archive.cache[number] = decoded
	archive.cacheOrder = append(archive.cacheOrder, number)
	if len(archive.cacheOrder) > clusterCacheMax {
		delete(archive.cache, archive.cacheOrder[0])
		archive.cacheOrder = archive.cacheOrder[1:]
	}
	return decoded, nil
}

func zimDocumentID(namespace byte, path string) string {
	digest := sha256Sum([]byte{namespace}, []byte(path))
	return "z" + strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(digest[:9]))
}

func sha256Sum(parts ...[]byte) [32]byte {
	var joined []byte
	for _, part := range parts {
		joined = append(joined, part...)
	}
	return sha256Bytes(joined)
}

// Kept behind a helper to make the stable-ID algorithm explicit in tests.
func sha256Bytes(data []byte) [32]byte { return sha256.Sum256(data) }

func parseCursor(after string, maximum uint32) (uint32, error) {
	if after == "" {
		return 0, nil
	}
	value, err := strconv.ParseUint(after, 10, 32)
	if err != nil || value > uint64(maximum) {
		return 0, fmt.Errorf("invalid ZIM scan cursor %q", after)
	}
	return uint32(value), nil
}
