// Package model holds the shared data types used across the store, scanner,
// and web packages.
package model

import "time"

type RootSource string

const (
	// RootSourceManual is a root added by hand through the UI.
	RootSourceManual RootSource = "manual"
	// RootSourceAuto is a root discovered under /scan on startup (see
	// Store.DiscoverRoots) rather than typed in by a user.
	RootSourceAuto RootSource = "auto"
)

type Root struct {
	ID      int64
	Path    string
	Label   string
	AddedAt time.Time
	Enabled bool
	Source  RootSource
}

type ScanStatus string

const (
	ScanRunning     ScanStatus = "running"
	ScanCompleted   ScanStatus = "completed"
	ScanFailed      ScanStatus = "failed"
	ScanInterrupted ScanStatus = "interrupted"
)

type ScanPhase string

const (
	PhaseWalking    ScanPhase = "walking"
	PhaseHashing    ScanPhase = "hashing"
	PhaseFinalizing ScanPhase = "finalizing"
)

type Scan struct {
	ID           int64
	StartedAt    time.Time
	FinishedAt   *time.Time
	Status       ScanStatus
	Phase        ScanPhase
	FilesSeen    int64
	FilesNew     int64
	FilesChanged int64
	FilesRemoved int64
	FilesSkipped int64
	BytesHashed  int64
	Error        string
	RootIDs      []int64
}

type FileStatus string

const (
	FileStatusPresent FileStatus = "present"
	FileStatusMissing FileStatus = "missing"
)

type FileRecord struct {
	ID             int64
	RootID         int64
	RelPath        string
	Size           int64
	MTimeNs        int64
	Device         uint64
	Inode          uint64
	PartialHash    string
	FullHash       string
	LastSeenScanID int64
	Status         FileStatus
	UpdatedAt      time.Time
}

type DuplicateGroup struct {
	ID                    int64
	Size                  int64
	FullHash              string
	MemberCount           int
	DistinctInstanceCount int
	ReclaimableBytes      int64
}

type DuplicateGroupMember struct {
	FileID   int64
	RootID   int64
	RootPath string
	RelPath  string
	Size     int64
	MTimeNs  int64
	Device   uint64
	Inode    uint64
}

// ProgressEvent is emitted by the scanner during a scan and pushed to
// subscribers (SSE clients) verbatim.
type ProgressEvent struct {
	Type         string `json:"type"` // scan_started | progress | scan_completed | scan_failed
	ScanID       int64  `json:"scan_id"`
	Phase        string `json:"phase,omitempty"`
	FilesSeen    int64  `json:"files_seen"`
	FilesNew     int64  `json:"files_new"`
	FilesChanged int64  `json:"files_changed"`
	FilesRemoved int64  `json:"files_removed"`
	FilesSkipped int64  `json:"files_skipped"`
	BytesHashed  int64  `json:"bytes_hashed"`
	CurrentPath  string `json:"current_path,omitempty"`
	Error        string `json:"error,omitempty"`
	ElapsedMs    int64  `json:"elapsed_ms"`
}
