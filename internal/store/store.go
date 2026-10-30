package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Alert is one due task_alerts row joined with its task and recipient.
type Alert struct {
	TaskID      int64
	RecipientID string
	AlertAt     time.Time
	ViaEmail    bool
	ViaPush     bool

	TaskTitle       string
	TaskDescription string
	TaskStatus      string
	TaskTag         string
	TaskStart       time.Time
	TaskEnd         time.Time

	ParentTaskID int64    // 0 unless the task is a subtask
	ParentTitle  string   // empty unless the task is a subtask
	Subtasks     []string // titles of the first maxSubtasks subtasks
	SubtaskCount int      // all subtasks, which can be more than len(Subtasks)
	OwnerName    string
	CreatorName  string // empty if the creator's account is gone
	Assignees    []string

	UserEmail string
	UserName  string   // public_info.name, else the part of the email before the @
	FCMTokens []string // from the recipient's unexpired sessions
}

const maxSubtasks = 10

// displayName is the SQL for a user's name, with the same fallback as the
// frontend (src/hooks/useUsers.ts).
func displayName(alias string) string {
	return fmt.Sprintf("COALESCE(NULLIF(%[1]s.public_info->>'name', ''), split_part(%[1]s.email, '@', 1))", alias)
}

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) SkipStale(ctx context.Context, olderThan time.Duration) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE task_alerts
		SET notification_sent = true
		WHERE notification_sent = false
		  AND alert_at < now() - make_interval(secs => $1)
	`, olderThan.Seconds())
	if err != nil {
		return 0, fmt.Errorf("skip stale alerts: %w", err)
	}
	return tag.RowsAffected(), nil
}

type Batch struct {
	tx     pgx.Tx
	Alerts []Alert
}

func (s *Store) ClaimDue(ctx context.Context, limit int, ahead time.Duration) (*Batch, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	b := &Batch{tx: tx}

	rows, err := tx.Query(ctx, `
		SELECT a.task_id, a.alert_recipient_id::text, a.alert_at,
		       a.notify_via_email, a.notify_via_push,
		       t.title, COALESCE(t.description, ''), t.task_status::text, t.task_tag::text,
		       t.start_at, t.end_at,
		       COALESCE(t.parent_task_id, 0), COALESCE(p.title, ''),
		       ARRAY(SELECT s.title FROM tasks s WHERE s.parent_task_id = t.task_id
		             ORDER BY s.start_at, s.task_id LIMIT `+fmt.Sprint(maxSubtasks)+`),
		       (SELECT count(*) FROM tasks s WHERE s.parent_task_id = t.task_id),
		       `+displayName("o")+`,
		       COALESCE(`+displayName("c")+`, ''),
		       ARRAY(SELECT `+displayName("au")+` FROM task_assignees ta
		             JOIN users au ON au.user_id = ta.assignee_id
		             WHERE ta.task_id = t.task_id ORDER BY 1),
		       u.email, `+displayName("u")+`
		FROM task_alerts a
		JOIN tasks t ON t.task_id = a.task_id
		JOIN users u ON u.user_id = a.alert_recipient_id
		JOIN users o ON o.user_id = t.owner_id
		LEFT JOIN users c ON c.user_id = t.created_by
		LEFT JOIN tasks p ON p.task_id = t.parent_task_id
		WHERE a.notification_sent = false
		  AND a.alert_at <= now() + make_interval(secs => $2)
		ORDER BY a.alert_at
		LIMIT $1
		FOR UPDATE OF a SKIP LOCKED
	`, limit, ahead.Seconds())
	if err != nil {
		b.Rollback(ctx)
		return nil, fmt.Errorf("claim due alerts: %w", err)
	}
	b.Alerts, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (Alert, error) {
		var a Alert
		err := row.Scan(&a.TaskID, &a.RecipientID, &a.AlertAt,
			&a.ViaEmail, &a.ViaPush,
			&a.TaskTitle, &a.TaskDescription, &a.TaskStatus, &a.TaskTag,
			&a.TaskStart, &a.TaskEnd,
			&a.ParentTaskID, &a.ParentTitle, &a.Subtasks, &a.SubtaskCount,
			&a.OwnerName, &a.CreatorName, &a.Assignees,
			&a.UserEmail, &a.UserName)
		return a, err
	})
	if err != nil {
		b.Rollback(ctx)
		return nil, fmt.Errorf("scan due alerts: %w", err)
	}

	if err := b.loadTokens(ctx); err != nil {
		b.Rollback(ctx)
		return nil, err
	}
	return b, nil
}

// loadTokens fills FCMTokens for every push alert with one query for the whole batch.
func (b *Batch) loadTokens(ctx context.Context) error {
	var userIDs []string
	for _, a := range b.Alerts {
		if a.ViaPush {
			userIDs = append(userIDs, a.RecipientID)
		}
	}
	if len(userIDs) == 0 {
		return nil
	}

	rows, err := b.tx.Query(ctx, `
		SELECT DISTINCT user_id::text, fcm_token
		FROM sessions
		WHERE user_id = ANY($1::text[]::uuid[])
		  AND fcm_token IS NOT NULL AND fcm_token <> ''
		  AND expires_at > now()
	`, userIDs)
	if err != nil {
		return fmt.Errorf("load fcm tokens: %w", err)
	}
	tokens := map[string][]string{}
	var userID, token string
	_, err = pgx.ForEachRow(rows, []any{&userID, &token}, func() error {
		tokens[userID] = append(tokens[userID], token)
		return nil
	})
	if err != nil {
		return fmt.Errorf("scan fcm tokens: %w", err)
	}

	for i := range b.Alerts {
		if b.Alerts[i].ViaPush {
			b.Alerts[i].FCMTokens = tokens[b.Alerts[i].RecipientID]
		}
	}
	return nil
}

// MarkSent flags one alert as sent. It takes effect on Commit.
func (b *Batch) MarkSent(ctx context.Context, a Alert) error {
	_, err := b.tx.Exec(ctx, `
		UPDATE task_alerts
		SET notification_sent = true
		WHERE task_id = $1 AND alert_recipient_id = $2::uuid AND alert_at = $3
	`, a.TaskID, a.RecipientID, a.AlertAt)
	if err != nil {
		return fmt.Errorf("mark alert sent (task %d): %w", a.TaskID, err)
	}
	return nil
}

func (b *Batch) Commit(ctx context.Context) error {
	return b.tx.Commit(ctx)
}

// Rollback releases the claimed rows. It's safe to call after Commit.
func (b *Batch) Rollback(ctx context.Context) {
	_ = b.tx.Rollback(ctx)
}
