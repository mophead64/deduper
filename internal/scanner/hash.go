package scanner

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"

	"github.com/zeebo/xxh3"
)

// sampleSize is how much of the head/middle/tail is read for the cheap
// partial-hash filter (README.md §6, Stage 2).
const sampleSize = 64 * 1024

// partialHash returns a fast, non-cryptographic hash over a sample of the
// file: the whole file if it's small, otherwise its head, a middle chunk, and
// its tail. Sampling three regions (not just the head) catches files that
// share an identical header or footer but differ in the middle.
func partialHash(path string, size int64) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := xxh3.New()

	if size <= sampleSize*3 {
		if _, err := io.Copy(h, f); err != nil {
			return "", err
		}
		return hashSum(h), nil
	}

	buf := make([]byte, sampleSize)

	if _, err := io.ReadFull(f, buf); err != nil {
		return "", err
	}
	h.Write(buf)

	mid := size/2 - sampleSize/2
	if _, err := f.Seek(mid, io.SeekStart); err != nil {
		return "", err
	}
	if _, err := io.ReadFull(f, buf); err != nil {
		return "", err
	}
	h.Write(buf)

	if _, err := f.Seek(-sampleSize, io.SeekEnd); err != nil {
		return "", err
	}
	if _, err := io.ReadFull(f, buf); err != nil {
		return "", err
	}
	h.Write(buf)

	return hashSum(h), nil
}

func hashSum(h *xxh3.Hasher) string {
	sum := h.Sum128()
	b := sum.Bytes()
	return hex.EncodeToString(b[:])
}

// fullHash streams the entire file through SHA-256 (README.md §6, Stage 4).
// This is the only step that reads a whole file end-to-end, and only runs on
// files that already survived the size and partial-hash filters.
func fullHash(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
