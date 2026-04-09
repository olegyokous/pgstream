package wal

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

// ListenerConfig holds configuration for the WAL listener.
type ListenerConfig struct {
	DSN            string
	SlotName       string
	PublicationName string
	StandbyTimeout time.Duration
}

// Listener tails PostgreSQL WAL changes via logical replication.
type Listener struct {
	cfg     ListenerConfig
	conn    *pgconn.PgConn
	decoder *Decoder
}

// NewListener creates a new Listener with the given config.
func NewListener(cfg ListenerConfig) *Listener {
	if cfg.StandbyTimeout == 0 {
		cfg.StandbyTimeout = 10 * time.Second
	}
	return &Listener{
		cfg:     cfg,
		decoder: NewDecoder(),
	}
}

// Connect establishes the replication connection.
func (l *Listener) Connect(ctx context.Context) error {
	conn, err := pgconn.Connect(ctx, l.cfg.DSN+" replication=database")
	if err != nil {
		return fmt.Errorf("listener: connect: %w", err)
	}
	l.conn = conn
	return nil
}

// Close terminates the replication connection.
func (l *Listener) Close(ctx context.Context) error {
	if l.conn != nil {
		return l.conn.Close(ctx)
	}
	return nil
}

// Start begins streaming WAL messages and sends decoded messages to out.
func (l *Listener) Start(ctx context.Context, out chan<- Message) error {
	sysident, err := pglogrepl.IdentifySystem(ctx, l.conn)
	if err != nil {
		return fmt.Errorf("listener: identify system: %w", err)
	}

	err = pglogrepl.StartReplication(ctx, l.conn, l.cfg.SlotName, sysident.XLogPos,
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				"proto_version '1'",
				fmt.Sprintf("publication_names '%s'", l.cfg.PublicationName),
			},
		})
	if err != nil {
		return fmt.Errorf("listener: start replication: %w", err)
	}

	clientXLogPos := sysident.XLogPos
	standbyDeadline := time.Now().Add(l.cfg.StandbyTimeout)

	for {
		if time.Now().After(standbyDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(ctx, l.conn,
				pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
			if err != nil {
				return fmt.Errorf("listener: standby update: %w", err)
			}
			standbyDeadline = time.Now().Add(l.cfg.StandbyTimeout)
		}

		ctxTimeout, cancel := context.WithDeadline(ctx, standbyDeadline)
		rawMsg, err := l.conn.ReceiveMessage(ctxTimeout)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			return fmt.Errorf("listener: receive: %w", err)
		}

		primaryMsg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		if primaryMsg.Data[0] == pglogrepl.PrimaryKeepaliveMessageByteID {
			continue
		}

		logicalMsg, err := pglogrepl.Parse(primaryMsg.Data[1:])
		if err != nil {
			return fmt.Errorf("listener: parse wal: %w", err)
		}

		msg, err := l.decoder.Decode(logicalMsg)
		if err != nil || msg == nil {
			continue
		}

		select {
		case out <- *msg:
		case <-ctx.Done():
			return ctx.Err()
		}

		if xld, ok := logicalMsg.(*pglogrepl.XLogDataV0); ok {
			clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
		}
	}
}
