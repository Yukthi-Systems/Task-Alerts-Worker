package notify

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Yukthi-Systems/Task-Alerts-Worker/internal/store"
)

// The formats V3-Notifications-Worker accepts (utils/handlers.py).
const (
	TypeEmail     = "email"
	TypePush      = "fcm"
	EmailTemplate = "task_alert" // in its utils/templates.py
	DataType      = "TASK_ALERT" // data.notification_type on push
)

// Email is the body of a "email" message: the consumer renders the template
// with these variables, and drops the message if any variable is missing.
type Email struct {
	To        string            `json:"to"`
	Template  string            `json:"template"`
	Variables map[string]string `json:"variables"`
}

// Push is the body of a "fcm" message, one per device token.
type Push struct {
	Token   string            `json:"token"`
	Title   string            `json:"title"`
	Body    string            `json:"body"`
	Data    map[string]string `json:"data"` // FCM data values must be strings
	AppName string            `json:"app_name"`
}

const (
	none           = "—"
	maxDescription = 1000
)

func EmailFor(a store.Alert, appURL string, now time.Time) Email {
	return Email{
		To:       a.UserEmail,
		Template: EmailTemplate,
		Variables: map[string]string{
			"name":        a.UserName,
			"task_title":  a.TaskTitle,
			"parent_task": orNone(a.ParentTitle),
			"when":        When(a.TaskStart, a.TaskEnd),
			"status":      label(a.TaskStatus),
			"tag":         label(a.TaskTag),
			"description": orNone(truncate(strings.TrimSpace(a.TaskDescription), maxDescription)),
			"subtasks":    list(a.Subtasks, a.SubtaskCount),
			"owner":       a.OwnerName,
			"created_by":  orNone(a.CreatorName),
			"assignees":   list(a.Assignees, len(a.Assignees)),
			"link":        Link(appURL, a),
			"year":        strconv.Itoa(now.Year()),
		},
	}
}

// PushesFor builds one push per device token.
func PushesFor(a store.Alert, appURL, appName string) []Push {
	body := When(a.TaskStart, a.TaskEnd)
	if a.ParentTitle != "" {
		body = "Subtask of " + a.ParentTitle + " · " + body
	}
	data := map[string]string{
		"notification_type": DataType,
		"task_id":           strconv.FormatInt(a.TaskID, 10),
		"link":              Link(appURL, a),
	}
	if a.ParentTaskID != 0 {
		data["parent_task_id"] = strconv.FormatInt(a.ParentTaskID, 10)
	}

	pushes := make([]Push, 0, len(a.FCMTokens))
	for _, token := range a.FCMTokens {
		pushes = append(pushes, Push{
			Token:   token,
			Title:   "Reminder: " + a.TaskTitle,
			Body:    body,
			Data:    data,
			AppName: appName,
		})
	}
	return pushes
}

// Link opens the app on the task. Calendar-FrontEnd doesn't read ?task= yet.
func Link(appURL string, a store.Alert) string {
	return appURL + "/?task=" + url.QueryEscape(strconv.FormatInt(a.TaskID, 10))
}

// an all-day date is stored as exactly midnight UTC.
func isAllDay(t time.Time) bool {
	t = t.UTC()
	return t.Hour() == 0 && t.Minute() == 0 && t.Second() == 0 && t.Nanosecond() == 0
}

const (
	dateFmt = "Mon, 2 Jan 2006"
	timeFmt = "15:04"
)

func When(start, end time.Time) string {
	start, end = start.UTC(), end.UTC()
	sameDay := start.Format(time.DateOnly) == end.Format(time.DateOnly)

	switch {
	case isAllDay(start) && isAllDay(end):
		if sameDay {
			return start.Format(dateFmt) + " (all day)"
		}
		return fmt.Sprintf("%s – %s (all day)", start.Format(dateFmt), end.Format(dateFmt))
	case sameDay:
		return fmt.Sprintf("%s, %s – %s UTC", start.Format(dateFmt), start.Format(timeFmt), end.Format(timeFmt))
	default:
		return fmt.Sprintf("%s, %s – %s, %s UTC",
			start.Format(dateFmt), start.Format(timeFmt), end.Format(dateFmt), end.Format(timeFmt))
	}
}

// label turns an enum value such as IN_PROGRESS into "In Progress".
func label(enum string) string {
	words := strings.Split(strings.ToLower(enum), "_")
	for i, w := range words {
		if w != "" {
			words[i] = strings.ToUpper(w[:1]) + w[1:]
		}
	}
	return strings.Join(words, " ")
}

// list joins names, noting how many more there are beyond those given.
func list(items []string, total int) string {
	if len(items) == 0 {
		return none
	}
	s := strings.Join(items, ", ")
	if more := total - len(items); more > 0 {
		s += fmt.Sprintf(" (+%d more)", more)
	}
	return s
}

func orNone(s string) string {
	if s == "" {
		return none
	}
	return s
}

func truncate(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max]) + "…"
}
