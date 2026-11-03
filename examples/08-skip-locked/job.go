package skiplocked

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

var (
	ErrNoJob      = errors.New("no eligible job available")
	ErrNotOwner   = errors.New("claim expired, revoked, or not owned by worker")
	ErrBadRequest = errors.New("invalid queue request")
)

type Job struct {
	ID             uuid.UUID
	QueueName      string
	Payload        string
	Status         string
	LockedBy       string
	ClaimToken     uuid.UUID
	AttemptCount   int
	ClaimedAt      time.Time
	LeaseExpiresAt time.Time
}

const (
	DefaultLeaseDuration = 30 * time.Second
	DefaultMaxAttempts   = 3
)
