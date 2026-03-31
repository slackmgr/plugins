package main

import (
	"encoding/base64"
	"encoding/json"

	"github.com/slackmgr/types"
)

// rawIssue wraps raw DynamoDB item attributes and implements types.Issue.
type rawIssue struct {
	channelID     string
	uniqueID      string
	correlationID string
	isOpen        bool
	postID        string
	body          json.RawMessage
}

func (i *rawIssue) MarshalJSON() ([]byte, error) { return i.body, nil }
func (i *rawIssue) ChannelID() string             { return i.channelID }
func (i *rawIssue) UniqueID() string              { return i.uniqueID }
func (i *rawIssue) GetCorrelationID() string      { return i.correlationID }
func (i *rawIssue) IsOpen() bool                  { return i.isOpen }
func (i *rawIssue) CurrentPostID() string         { return i.postID }

// rawMoveMapping wraps raw DynamoDB item attributes and implements types.MoveMapping.
type rawMoveMapping struct {
	channelID     string
	correlationID string
	body          json.RawMessage
}

func (m *rawMoveMapping) MarshalJSON() ([]byte, error) { return m.body, nil }
func (m *rawMoveMapping) ChannelID() string            { return m.channelID }
func (m *rawMoveMapping) UniqueID() string {
	// Mirrors the core module convention: base64(channelID#correlationID).
	return base64.URLEncoding.EncodeToString([]byte(m.channelID + "#" + m.correlationID))
}
func (m *rawMoveMapping) GetCorrelationID() string { return m.correlationID }

// Ensure interfaces are satisfied at compile time.
var _ types.Issue = (*rawIssue)(nil)
var _ types.MoveMapping = (*rawMoveMapping)(nil)
