package kitsune

type fileEvent struct {
	Size     int64  `json:"size,omitempty"`
	Filename string `json:"filename,omitempty"`
}
